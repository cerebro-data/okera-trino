package com.okera.recordservice.trino;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import com.okera.recordservice.core.ByteArray;
import com.okera.recordservice.core.ConnectionCache;
import com.okera.recordservice.core.ConnectionContext;
import com.okera.recordservice.core.FetchResult;
import com.okera.recordservice.core.LocalityUtil;
import com.okera.recordservice.core.NetworkAddress;
import com.okera.recordservice.core.RecordServiceException;
import com.okera.recordservice.core.RecordServicePlannerClient;
import com.okera.recordservice.core.RecordServiceWorkerClient;
import com.okera.recordservice.core.Records;
import com.okera.recordservice.core.Schema;
import com.okera.recordservice.core.Schema.ColumnDesc;
import com.okera.recordservice.core.Task;
import com.okera.recordservice.util.ClusterUtil;
import com.okera.recordservice.util.Preconditions;
import io.airlift.concurrent.SetThreadName;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

/**
 * Utility connector APIs. All methods should be static.
 */
public class RecordServiceUtil {
  // private static final Logger LOG = LoggerFactory.getLogger(RecordServiceUtil.class);

  public static int TIMESTAMP_SCALE_FACTOR = 1;

  // This function might be called from multiple threads, but we
  // don't mind as they should all be calling with the same value,
  // and all readers will read this value after the first time it's
  // set, on the same calling thread.
  public static void setVersion(String version) {
    if (version.compareToIgnoreCase("338") > 0) {
      // In versions higher than 338, timestamps need to be provided in
      // epoch micros rather than epoch millis.
      TIMESTAMP_SCALE_FACTOR = 1000;
    }
  }

  /**
   * Creates a Records object for this split.
   */
  @SuppressWarnings("resource")
  public static Records createRecords(ConnectorSession session,
      RecordServiceSplit split, FetchResult.RecordFormat format)
      throws TrinoException {
    Task task = split.getTask();
    Records records;
    RecordServiceWorkerClient worker = null;
    try {
      boolean isAllowExternalExecution = ClusterUtil.isTaskColocated(
          split.getLocalWorkerPort(), task.allowExternalTaskExecution);
      boolean disableWorkerAuth = isAllowExternalExecution &&
          !split.isForceWorkerAuth();
      NetworkAddress host = LocalityUtil.getWorkerToConnectTo(
          task.taskId, task.localHosts, task.remoteHosts, split.getLocalWorkerPort(),
          isAllowExternalExecution);

      // TODO: add the username to the split so that we can pass
      // it in to setToken here to enable token refresh in this
      // scenario.
      ConnectionContext ctx = new ConnectionContext(RecordServiceConfig.TRINO_APP)
          .setHostPort(host);
      RecordServiceUtil.populateRequestContext(ctx, session);

      // Set token info if worker is not going to run on co-located node, unless forced
      // by flag.
      if (!disableWorkerAuth)  {
        ctx = ctx.setToken(split.getServiceName(), split.getToken());
      }


      ctx.setEnableCompression(split.isCompressionEnabled());
      if (split.recordsQueueSize() > 0) {
        ctx.setRecordsQueueSize(split.recordsQueueSize());
      }
      if (format != null && split.getFormats().contains(format)) {
        ctx.setRecordFormat(format);
      }
      if (split.getLimit() > 0) {
        ctx.setLimit(split.getLimit());
      }
      if (split.getWorkerRpcTimeoutMs() > 0)  {
        ctx.setRpcTimeoutMs(split.getWorkerRpcTimeoutMs());
      }
      if (split.getWorkerTaskMemLimit() > 0)  {
        ctx.setMemLimit(split.getWorkerTaskMemLimit());
      }
      if (split.getWorkerDefaultPoolMemLimit() > 0) {
        ctx.setDefaultPoolMemLimit(split.getWorkerDefaultPoolMemLimit());
      }
      if (split.isEnableWorkerSSL()) {
        ctx.setSSL(split.isEnableWorkerSSL());
      }

      // If this is a colocated task, create a connection to the planner and refresh
      // the task if needed. eg. if task has S3 pre-signed URL
      // Use the token available to the worker for authenticating planner connection.
      // TODO: This currently checks if the task returned allows Extern execution.
      // This is different from if the task can be refreshed. Planner connection formation
      // and RPC are expensive, hence this is needed for perf.
      // We ideally should be returning a new field in task, "is_refreshable_task" that is
      // set to true only for s3 files for now, and use that to check if task should be
      // refreshed here.
      // The reason for this is, allowExternalTaskExecution is client-behavior dependent
      // (we can allow all tasks to execute remotely via flag) while refresh is table
      // dependent.
      if (task.allowExternalTaskExecution && RecordServicePlannerClient.shouldRefreshTask(
            task, split.getPlannerTaskMaxRefreshSec()))  {
        ConnectionContext planCtx = new ConnectionContext(RecordServiceConfig.TRINO_APP)
            .setHostPort(split.getPlannerHost(), split.getPlannerPort())
            .setToken(split.getServiceName(), split.getToken())
            .setSSL(split.isEnablePlannerSSL());
        RecordServiceUtil.populateRequestContext(planCtx, session);
        try (RecordServicePlannerClient planner = ConnectionCache.defaultCache()
            .getPlanner(split.getConnectedUser(), null, planCtx))  {
          task = planner.refreshTaskCredentials(task);
        }
      }

      try (SetThreadName ignored = new SetThreadName(
          "Query-%s -- Task-%x-%x", Thread.currentThread().getName(), task.taskId.hi,
          task.taskId.lo);) {
        worker = new RecordServiceWorkerClient(ctx);
        records = worker.execAndFetch(task);
      }

      // Given how Presto manages its connections, we want to close the underlying worker
      // when we close a given RecordSet.
      records.setCloseWorker(true);
      worker = null;
    } catch (RecordServiceException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to execute task.\n" + e.getMessage(), e);
    } catch (IOException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to execute task.\n" + e.getMessage(), e);
    } finally {
      if (worker != null) worker.close();
    }
    return records;
  }

  /**
   * Converts a ByteArray to a Presto Slice
   */
  public static Slice toSlice(ByteArray ba) {
    return Slices.wrappedBuffer(ba.byteBuffer().array(), ba.offset(), ba.len());
  }

  /**
   * Converts RecordServiceColumnHandles to Trino types.
   */
  public static List<Type> getColumnTypes(List<RecordServiceColumnHandle> columns) {
    List<Type> types = new ArrayList<Type>();
    for (RecordServiceColumnHandle col: columns) {
      types.add(col.columnType());
    }
    return types;
  }

  /**
   * Converts an array to a trino equivalent (block).
   */
  public static Block convertArray(Records.Array array, Type arrayType,
      ColumnDesc col, BlockBuilder parent) {
    int numElements = array.nextLength();
    BlockBuilder builder = null;
    Type itemType = arrayType.getTypeParameters().get(0);

    if (parent != null) {
      builder = parent.beginBlockEntry();
    } else {
      builder = itemType.createBlockBuilder(null, numElements);
    }

    for (int i = 0; i < numElements; ++i) {
      // Structs cannot have a null value, so we have to check them before we
      // ask the array if the value is null.
      if (col.fields.get(0).type.typeId == Schema.Type.RECORD) {
        convertRecord(array.nextStruct(), itemType, col.fields.get(0), builder);
        continue;
      }

      if (array.isNull(i)) {
        builder.appendNull();
        continue;
      }
      switch (col.fields.get(0).type.typeId) {
        case BOOLEAN:
          BooleanType.BOOLEAN.writeBoolean(builder, array.nextBoolean());
          break;
        case TINYINT:
          IntegerType.INTEGER.writeLong(builder, array.nextByte());
          break;
        case SMALLINT:
          IntegerType.INTEGER.writeLong(builder, array.nextShort());
          break;
        case INT:
          IntegerType.INTEGER.writeLong(builder, array.nextInt());
          break;
        case BIGINT:
          BigintType.BIGINT.writeLong(builder, array.nextLong());
          break;

        case FLOAT:
          DoubleType.DOUBLE.writeDouble(builder, array.nextFloat());
          break;
        case DOUBLE:
          DoubleType.DOUBLE.writeDouble(builder, array.nextDouble());
          break;

        case DATE:
          DateType.DATE.writeLong(builder, array.nextInt());
          break;

        case VARCHAR:
        case CHAR:
        case BINARY:
        case STRING:
          itemType.writeSlice(builder,
              RecordServiceUtil.toSlice(array.nextByteArray()));
          break;

        case DECIMAL:
          DecimalType dt = (DecimalType) itemType;
          if (dt.isShort()) {
            dt.writeLong(builder, array.nextDecimal().toBigInteger().longValue());
          } else {
            dt.writeObject(builder, Int128.valueOf(
                array.nextDecimal().toBigInteger()));
          }
          break;
        case TIMESTAMP_NANOS:
          TimestampType.TIMESTAMP_NANOS.writeLong(builder,
              array.nextTimestampNanos().getMillisSinceEpoch() * TIMESTAMP_SCALE_FACTOR);
          break;

        case UNSUPPORTED:
        case RECORD:
          // Should not have gotten here (Handled above)
          Preconditions.checkState(false);
          break;

        case ARRAY:
          convertArray(array.nextArray(), itemType, col.fields.get(0), builder);
          break;

        case MAP:
          convertMap(array.nextMap(), itemType, col.fields.get(0), builder);
          break;
      }
    }
    if (parent != null) {
      parent.closeEntry();
      return null;
    } else {
      return builder.build();
    }
  }

  /**
    * Converts an map to a trino equivalent (block).
    */
  public static Block convertMap(Records.Map map, Type mapType,
      ColumnDesc col, BlockBuilder parent) {
    int numElements = map.nextLength();
    BlockBuilder builder = null;

    Type keyType = mapType.getTypeParameters().get(0);
    Type valueType = mapType.getTypeParameters().get(1);

    if (parent != null) {
      builder = parent;
    } else {
      builder = mapType.createBlockBuilder(null, numElements);
    }
    BlockBuilder singleMapBuild = builder.beginBlockEntry();

    for (int i = 0; i < numElements; ++i) {
      // Serialize key
      keyType.writeSlice(singleMapBuild, RecordServiceUtil.toSlice(map.nextKey()));

      // Structs cannot have a null value, so we have to check them before we
      // ask the map if the value is null.
      if (col.fields.get(1).type.typeId == Schema.Type.RECORD) {
        convertRecord(map.nextStructValue(), valueType,
            col.fields.get(1), singleMapBuild);
        continue;
      }

      // Serialize value
      if (map.isValueNull(i)) {
        singleMapBuild.appendNull();
        continue;
      }
      switch (col.fields.get(1).type.typeId) {
        case BOOLEAN:
          BooleanType.BOOLEAN.writeBoolean(singleMapBuild, map.nextBooleanValue());
          break;
        case TINYINT:
          IntegerType.INTEGER.writeLong(singleMapBuild, map.nextByteValue());
          break;
        case SMALLINT:
          IntegerType.INTEGER.writeLong(singleMapBuild, map.nextShortValue());
          break;
        case INT:
          IntegerType.INTEGER.writeLong(singleMapBuild, map.nextIntValue());
          break;
        case BIGINT:
          BigintType.BIGINT.writeLong(singleMapBuild, map.nextLongValue());
          break;

        case FLOAT:
          DoubleType.DOUBLE.writeDouble(singleMapBuild, map.nextFloatValue());
          break;
        case DOUBLE:
          DoubleType.DOUBLE.writeDouble(singleMapBuild, map.nextDoubleValue());
          break;

        case DATE:
          DateType.DATE.writeLong(singleMapBuild, map.nextIntValue());
          break;

        case VARCHAR:
        case CHAR:
        case BINARY:
        case STRING:
          valueType.writeSlice(singleMapBuild,
              RecordServiceUtil.toSlice(map.nextByteArrayValue()));
          break;

        case DECIMAL:
          DecimalType dt = (DecimalType) valueType;
          if (dt.isShort()) {
            dt.writeLong(singleMapBuild,
                map.nextDecimalValue().toBigInteger().longValue());
          } else {
            dt.writeObject(singleMapBuild, Int128.valueOf(
                map.nextDecimalValue().toBigInteger()));
          }
          break;
        case TIMESTAMP_NANOS:
          TimestampType.TIMESTAMP_NANOS.writeLong(singleMapBuild,
              map.nextTimestampNanosValue().getMillisSinceEpoch() * TIMESTAMP_SCALE_FACTOR);
          break;

        case UNSUPPORTED:
        case RECORD:
          // Should not have gotten here (Handled above)
          Preconditions.checkState(false);
          break;

        case ARRAY:
          RecordServiceUtil.convertArray(map.nextArrayValue(),
              valueType, col.fields.get(1), singleMapBuild);
          break;

        case MAP:
          convertMap(map.nextMapValue(),
              valueType, col.fields.get(1), singleMapBuild);
          break;
      }
    }
    builder.closeEntry();
    if (parent != null) {
      return null;
    } else {
      return (Block)mapType.getObject(builder, 0);
    }
  }

  /**
    * Converts a struct to a trino equivalent (block). This is called recursively.
    */
  public static Block convertRecord(Records.Struct record,
      Type type, ColumnDesc col, BlockBuilder parent) {
    boolean topLevelBuilder = false;
    if (parent == null) {
      parent = type.createBlockBuilder(null, 1);
      topLevelBuilder = true;
    }
    BlockBuilder builder = parent.beginBlockEntry();

    for (int i = 0; i < record.numFields(); i++) {
      Type childType = type.getTypeParameters().get(i);
      if (col.fields.get(i).type.typeId == Schema.Type.RECORD) {
        // Records are non-nullable.
        convertRecord(record.nextStruct(i), childType, col.fields.get(i), builder);
        continue;
      }
      if (record.isNull(i)) {
        builder.appendNull();
        continue;
      }

      switch (col.fields.get(i).type.typeId) {
        case BOOLEAN:
          BooleanType.BOOLEAN.writeBoolean(builder, record.nextBoolean(i));
          break;

        case TINYINT:
          IntegerType.INTEGER.writeLong(builder, record.nextByte(i));
          break;
        case SMALLINT:
          IntegerType.INTEGER.writeLong(builder, record.nextShort(i));
          break;
        case INT:
          IntegerType.INTEGER.writeLong(builder, record.nextInt(i));
          break;
        case BIGINT:
          BigintType.BIGINT.writeLong(builder, record.nextLong(i));
          break;

        case FLOAT:
          DoubleType.DOUBLE.writeDouble(builder, record.nextFloat(i));
          break;
        case DOUBLE:
          DoubleType.DOUBLE.writeDouble(builder, record.nextDouble(i));
          break;

        case DATE:
          DateType.DATE.writeLong(builder, record.nextInt(i));
          break;

        case VARCHAR:
        case CHAR:
        case BINARY:
        case STRING:
          childType.writeSlice(builder,
              RecordServiceUtil.toSlice(record.nextByteArray(i)));
          break;

        case DECIMAL:
          DecimalType dt = (DecimalType) childType;
          if (dt.isShort()) {
            dt.writeLong(builder, record.nextDecimal(i).toBigInteger().longValue());
          } else {
            dt.writeObject(builder, Int128.valueOf(
                record.nextDecimal(i).toBigInteger()));
          }
          break;
        case TIMESTAMP_NANOS:
          TimestampType.TIMESTAMP_NANOS.writeLong(builder,
              record.nextTimestampNanos(i).getMillisSinceEpoch() * TIMESTAMP_SCALE_FACTOR);
          break;

        case ARRAY:
          RecordServiceUtil.convertArray(
              record.getArray(i), childType, col.fields.get(i), builder);
          break;

        case MAP:
          convertMap(record.getMap(i), childType, col.fields.get(i), builder);
          break;

        case UNSUPPORTED:
        case RECORD:
          // Should not have gotten here (Handled above)
          Preconditions.checkState(false);
          break;
      }
    }
    parent.closeEntry();
    if (topLevelBuilder) {
      return (Block)type.getObject(parent, 0);
    } else {
      return null;
    }
  }

  /**
   * Returns the connected user's name for a given session.
   */
  public static String getConnectedUserName(ConnectorSession session) {
    if (System.getenv("OKERA_ALLOW_IMPERSONATION") == null) {
      return session.getUser();
    } else  {
      return System.getProperty("user.name");
    }
  }

  /**
   * Populate connection context with metadata from session.
   */
  public static void populateRequestContext(ConnectionContext ctx,
      ConnectorSession session) {
    ctx.setRequestId(session.getQueryId());
    if (session.getTraceToken().isPresent()) {
      ctx.setRequestName(session.getTraceToken().get());
    }
  }
}

