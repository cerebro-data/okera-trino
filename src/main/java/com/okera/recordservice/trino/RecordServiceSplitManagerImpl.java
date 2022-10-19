// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.okera.recordservice.trino;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import com.google.common.base.Joiner;

import com.okera.recordservice.core.NetworkAddress;
import com.okera.recordservice.core.PlanRequestResult;
import com.okera.recordservice.core.RecordServiceException;
import com.okera.recordservice.core.RecordServicePlannerClient;
import com.okera.recordservice.core.Request;
import com.okera.recordservice.core.Schema;
import com.okera.recordservice.core.Task;
import com.okera.recordservice.util.Preconditions;

import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RecordServiceSplitManagerImpl {
  private static final Logger LOG = Logger.get(RecordServiceSplitManagerImpl.class);

  // Thread pool to compute splits/plan requests
  // We want controlled parallelism.
  private final ExecutorService executor = Executors.newFixedThreadPool(16);

  private final class SplitSource implements ConnectorSplitSource {

    private final Future<List<ConnectorSplit>> splitsFuture_;
    private List<ConnectorSplit> splits_;
    int splitResultIdx_ = 0;

    public SplitSource(
        ConnectorSession session, RecordServiceTableHandle table,
        Request request, RecordServiceConfig config, long limit) {
      splitsFuture_ = executor.submit(new Callable<List<ConnectorSplit>>() {
        @Override
        public List<ConnectorSplit> call() {
          return compute(session, table, request, config, limit);
        }
      });
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) {
      if (splits_ == null) {
        try {
          splits_ = splitsFuture_.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error(e);
          String message = "Could not compute splits";
          // If we have a known good message from the cause exception,
          // then use that instead of the more generic message. Without
          // this, the original message is only logged and not visible
          // to the user.
          if (e.getCause() != null) {
            if (e.getCause() instanceof TrinoException) {
              message = ((TrinoException)e.getCause()).getMessage();
            }
          }
          throw new TrinoException(GENERIC_INTERNAL_ERROR,
              message, e);
        }
      }
      int toReturn = Math.min(maxSize, splits_.size() - splitResultIdx_);
      List<ConnectorSplit> results = splits_.subList(
          splitResultIdx_, splitResultIdx_ + toReturn);
      splitResultIdx_ += toReturn;
      return CompletableFuture.completedFuture(
          new ConnectorSplitBatch(results, isFinished()));
    }

    @Override
    public void close() {
      // No-op
    }

    @Override
    public boolean isFinished() {
      return splitsFuture_.isDone() && splitResultIdx_ == splits_.size();
    }

    private List<ConnectorSplit> compute(
        ConnectorSession session, RecordServiceTableHandle table,
        Request request, RecordServiceConfig config, long limit) {
      PlanRequestResult plan;
      String token;
      try (SetThreadName ignored = new SetThreadName("Query-%s", session.getQueryId());
          RecordServicePlannerClient planner = config.getPlanner(session, nodeManager)) {
        plan = planner.planRequest(request, (config.getLocalWorkerPort() > 0),
            config.isAllowExternalRestrictedTasks(), false);
        token = planner.getToken();
        LOG.info("Supported server record formats: " + plan.supportedFormats);
      } catch (IOException e) {
        LOG.error(e);
        String message = "Error planning request: " + e.getMessage();
        throw new TrinoException(GENERIC_INTERNAL_ERROR, message, e);
      } catch (RecordServiceException e) {
        LOG.error(e);
        String message = "Error planning request: " + e.message + ": " + e.detail;
        throw new TrinoException(GENERIC_INTERNAL_ERROR, message, e);
      }

      // Convert plan to trino splits
      List<ConnectorSplit> splits = new ArrayList<ConnectorSplit>();
      for (int i = 0; i < plan.tasks.size(); i++) {
        Task task = plan.tasks.get(i);
        // TODO: take a random subset. We don't need to send the entire membership
        // per task.
        task.setRemoteHosts(plan.hosts);
        List<HostAddress> hosts = new ArrayList<HostAddress>();
        for (NetworkAddress addr: task.localHosts) {
          hosts.add(HostAddress.fromParts(addr.hostname, addr.port));
        }

        // Find the connected user for this ConnectorSession and add to split.
        String connectedUser = RecordServiceUtil.getConnectedUserName(session);
        try {
          splits.add(new RecordServiceSplit(config.getHost(), config.getPort(),
              connectedUser, config.getWorkerRpcTimeoutMs(),
              config.getLocalWorkerPort(),
              table, i, plan.tasks.size(), hosts,
              task.serialize(), config.getServiceName(), token,
              config.isCompressionEnabled(), plan.supportedFormats, limit,
              config.getRecordsQueueSize(),
              containsComplexTypes(plan.schema),
              config.getPlannerTaskMaxRefreshSec(),
              config.getWorkerTaskMemLimit(),
              config.getWorkerDefaultPoolMemLimit(),
              config.isForceWorkerAuth(),
              config.isPlannerSSL(),
              config.isWorkerSSL()));
        } catch (IOException e) {
          throw new TrinoException(
            GENERIC_INTERNAL_ERROR, "Unable to serialize task.", e);
        }
      }
      return splits;
    }
  }

  private final RecordServiceConfig config;
  private final NodeManager nodeManager;

  // If non-null, the white list of columns where we will generate equality filters
  // on. Columns not in this will not do filter push down. This is to increase the
  // effectiveness of the cache.
  // Note that null and empty have very different behaviors will null has this
  // disabled and all filters are pushed down and an empty white list will do no push
  // down.
  private static final Set<String> EQ_FILTER_COLUMN_WHITE_LIST;
  private static final Set<String> FILTER_COLUMN_BLACK_LIST = new HashSet<>();

  private static final String EQ_FILTER_COLUMN_CONFIG =
      "OKERA_EQ_FILTER_COLUMN_WHITE_LIST";
  private static final String BLACK_LIST_FILTER_COLUMN_CONFIG =
      "OKERA_FILTER_COLUMN_BLACK_LIST";

  static {
    if (System.getenv().containsKey(EQ_FILTER_COLUMN_CONFIG)) {
      EQ_FILTER_COLUMN_WHITE_LIST = new HashSet<>();
      for (String col: System.getenv(EQ_FILTER_COLUMN_CONFIG).split(",")) {
        EQ_FILTER_COLUMN_WHITE_LIST.add(col.toLowerCase());
      }
    } else {
      EQ_FILTER_COLUMN_WHITE_LIST = null;
    }
    if (System.getenv().containsKey(BLACK_LIST_FILTER_COLUMN_CONFIG)) {
      for (String col: System.getenv(BLACK_LIST_FILTER_COLUMN_CONFIG).split(",")) {
        FILTER_COLUMN_BLACK_LIST.add(col.toLowerCase());
      }
    }
  }

  public RecordServiceSplitManagerImpl(RecordServiceConfig config,
      NodeManager nodeManager) {
    this.config = config;
    this.nodeManager = nodeManager;
  }

  /**
   * Returns the splits.
   */
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
      ConnectorSession session, ConnectorTableHandle table,
      DynamicFilter dynamicFilter, Constraint constraint) { // TODO: What to do with new params? Dynamic filtering?
    RecordServiceTableHandle handle = (RecordServiceTableHandle) table;
    Optional<List<ColumnHandle>> cols = handle.getDesiredColumns();
    SchemaTableName name = handle.getSchemaTableName();
    final String tbl = Request.escapeDbTable(name.toString());

    Request request;
    if (cols.isPresent()) {
      List<String> projection = new ArrayList<String>();
      List<RecordServiceColumnHandle> columnHandles =
          new ArrayList<RecordServiceColumnHandle>();
      for (ColumnHandle h: cols.get()) {
        RecordServiceColumnHandle col = (RecordServiceColumnHandle)h;
        columnHandles.add(col);
        if (col.columnType() instanceof RowType) {
          // Since we're appending the .*, we need to handle escaping
          // ourselves.
          projection.add(Request.quote(col.columnName()) + ".*");
        } else {
          projection.add(col.columnName());
        }
      }
      request = Request.createProjectionRequest(tbl, projection);

      List<String> clauses = toConjuncts(columnHandles, handle.getConstraint());
      if (!clauses.isEmpty()) {
        request.addPredicates(Joiner.on(" AND ").join(clauses));
      }
    } else {
      request = Request.createRequest(tbl);
    }

    long limit;
    try {
      limit = session.getProperty(
          RecordServiceSessionProperties.LIMIT,
          Long.class
      );
      if (limit != -1) {
        LOG.info("Setting limit to " + limit + " from session property");
      }
    } catch (TrinoException e) {
      limit = -1;
    }

    return new SplitSource(session, handle, request, config, limit);
  }

  private static boolean containsComplexTypes(Schema schema) {
    List<Schema.ColumnDesc> schemaCols = schema.nestedCols != null ?
        schema.nestedCols : schema.cols;
    for (int colIdx = 0; colIdx < schemaCols.size(); colIdx++) {
      Schema.TypeDesc type = schemaCols.get(colIdx).type;
      if (type.typeId == Schema.Type.ARRAY || type.typeId == Schema.Type.MAP ||
          type.typeId == Schema.Type.RECORD) {
        return true;
      }
    }
    return false;
  }

  /**
   * Turns tupleDomain into a list of conjuncts.
   */
  private List<String> toConjuncts(List<RecordServiceColumnHandle> columns,
      TupleDomain<ColumnHandle> tupleDomain) {
    List<String> builder = new ArrayList<String>();
    for (RecordServiceColumnHandle column : columns) {
      Type type = column.columnType();
      if (!acceptedType(type)) continue;
      Domain domain = tupleDomain.getDomains().get().get(column);
      if (domain != null) {
        String predicate = toPredicate(column.columnName(), domain, type);
        if (!predicate.isEmpty()) builder.add(predicate);
      }
    }
    return builder;
  }


  /**
   * Generates the predicate columnName [operator] value
   */
  private String toPredicate(
      String columnName, String operator, Object value, Type type) {
    if (type instanceof VarcharType) {
      return Request.quote(columnName) + " " +
          operator + " '" + ((Slice) value).toStringUtf8() + "'";
    } else if (type == DateType.DATE) {
      return Request.quote(columnName) + " " +
          operator + " CAST(" + value + " AS DATE)";
    } else {
      return Request.quote(columnName) + " " + operator + " " + value;
    }
  }

  /**
   * Creates a single predicate for columnName.
   * Returns an empty string if no filter is generated
   */
  private String toPredicate(String columnName, Domain domain, Type type) {
    Preconditions.checkArgument(
        domain.getType().isOrderable(), "Domain type must be orderable");

    if (FILTER_COLUMN_BLACK_LIST.contains(columnName.toLowerCase())) return "";

    if (domain.getValues().isNone()) {
      return domain.isNullAllowed() ? Request.quote(columnName) + " IS NULL" : "FALSE";
    }

    if (domain.getValues().isAll()) {
      return domain.isNullAllowed() ? "TRUE" : Request.quote(columnName) + " IS NOT NULL";
    }

    List<String> disjuncts = new ArrayList<>();
    List<Object> singleValues = new ArrayList<>();
    for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
      Preconditions.checkState(!range.isAll()); // Already checked
      if (range.isSingleValue()) {
        singleValues.add(range.getSingleValue());
      } else {
        List<String> rangeConjuncts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
          rangeConjuncts.add(
            toPredicate(columnName, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), type));
        }
        if (!range.isHighUnbounded()) {
          rangeConjuncts.add(
            toPredicate(columnName, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), type));
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have
        // been checked for
        Preconditions.checkState(!rangeConjuncts.isEmpty());
        disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
      }
    }

    // Add back all of the possible single values either as an equality or an IN predicate
    if (EQ_FILTER_COLUMN_WHITE_LIST == null ||
        EQ_FILTER_COLUMN_WHITE_LIST.contains(columnName.toLowerCase())) {
      if (singleValues.size() == 1) {
        disjuncts.add(toPredicate(columnName, "=", singleValues.get(0), type));
      } else if (singleValues.size() > 1) {
        String values;
        if (type instanceof VarcharType) {
          List<String> strValues = new ArrayList<String>();
          for (Object o : singleValues) {
            strValues.add("'" + ((Slice) o).toStringUtf8() + "'");
          }
          values = Joiner.on(",").join(strValues);
        } else if (type == DateType.DATE) {
          List<String> strValues = new ArrayList<String>();
          for (Object o : singleValues) {
            strValues.add("CAST(" + o + " AS DATE)");
          }
          values = Joiner.on(",").join(strValues);
        } else {
          values = Joiner.on(",").join(singleValues);
        }
        disjuncts.add(Request.quote(columnName) + " IN (" + values + ")");
      }
    }

    // Add nullability disjuncts
    if (domain.isNullAllowed()) {
      disjuncts.add(Request.quote(columnName) + " IS NULL");
    }

    if (disjuncts.isEmpty()) return "";
    return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
  }

  /**
   * Returns true if we support pushing predicates for this type.
   * TODO: determine if timestamp works.
   */
  private boolean acceptedType(Type type) {
    return type.equals(BooleanType.BOOLEAN) ||
        type.equals(TinyintType.TINYINT) ||
        type.equals(SmallintType.SMALLINT) ||
        type.equals(IntegerType.INTEGER) ||
        type.equals(BigintType.BIGINT) ||
        type.equals(DoubleType.DOUBLE) ||
        type.equals(DateType.DATE) ||
        type instanceof VarcharType ||
        type instanceof CharType;
  }
}
