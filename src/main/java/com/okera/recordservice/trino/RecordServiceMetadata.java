// Copyright Okera Inc.
//
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
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.okera.recordservice.core.Database;
import com.okera.recordservice.core.RecordServiceException;
import com.okera.recordservice.core.RecordServicePlannerClient;
import com.okera.recordservice.core.Schema;
import com.okera.recordservice.core.Table;
import com.okera.recordservice.util.BoundedCache;
import com.okera.recordservice.util.MetadataCache;
import com.okera.recordservice.util.ParseUtil;

import io.airlift.log.Logger;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

import org.apache.commons.lang.StringEscapeUtils;

public class RecordServiceMetadata implements ConnectorMetadata {

  public static final String INFORMATION_SCHEMA = "information_schema";
  public static final String OKERA_CATALOG = "okera";
  public static final int MAX_SCHEMAS = 100;
  public static final int MAX_TABLES_PER_SCHEMA = 50;
  private static final Logger LOG = Logger.get(RecordServiceMetadata.class);

  private final RecordServiceModule module;
  private final RecordServiceConfig config;
  private final TypeManager typeManager;

  // Cache of basic metadata. This cache is maintained per user.
  // Can be null if caching is disabled.
  // TODO: this currently leaks and never removes disconnected sessions.
  // We need to add either another thread or maintain TTL with another
  // data structure.
  private final Map<String, MetadataCache> metadataCache_;

  // If greater than zero, cache for basic metadata (database and table names).
  private final int metadataCacheTtlMs_;

  // Per query metadata caches. This provides both performance and snapshot consistency
  // This size doesn't matter a lot as it is the maximum of in flight queries.
  private static final int QUERY_CACHE_SIZE = 512;
  private final BoundedCache<List<SchemaTableName>> perQueryTableNameCache_;
  private final BoundedCache<List<SchemaTableName>> perQueryViewNameCache_;
  private final BoundedCache<Map<String,Table>> perQueryTablesCache_;

  // If false, enable external view support for presto. In this case, we would return
  // the view definition for catalog objects that are external views. This doesn't
  // seem very well supported by the presto connector API. While it has the functions
  // it uses internal objects for view serialization and in general not as compatible
  // with hiveql. This needs more investigation to make sure it is compatible.
  //
  // If disabled (which was the historical default), then we treat it as a table
  // and the view is evaluated in Okera (i.e. it actually is handled like an internal
  // view).
  private boolean disableExternalViews_;

  @Inject
  public RecordServiceMetadata(RecordServiceModule module,
      RecordServiceConfig config, TypeManager typeManager) {
    this.module = module;
    this.config = config;
    this.typeManager = typeManager;

    disableExternalViews_ =
        "false".equalsIgnoreCase(System.getenv("OKERA_ENABLE_EXTERNAL_VIEWS"));

    if (config.isEnableExternalViews() != null)  {
      // Only check if the okera connector property is set
      LOG.info("enable-external-views set from Okera connector to: " +
          config.isEnableExternalViews());
      disableExternalViews_ = Boolean.FALSE.equals(config.isEnableExternalViews());
    }

    if (System.getenv("OKERA_METADATA_CACHE_TTL_MS") != null) {
      metadataCacheTtlMs_ = Integer.parseInt(
          System.getenv("OKERA_METADATA_CACHE_TTL_MS"));
    } else {
      metadataCacheTtlMs_ = config.getMetadataCacheTtlMs();
    }

    if (metadataCacheTtlMs_ > 0) {
      if (!disableExternalViews_) {
        LOG.info("Metadata cache cannot be enabled because external views " +
            "are also enabled and this combination is not supported.");
        metadataCache_ = null;
      } else {
        LOG.info("Metadata cache enabled by with TTL (ms): " + metadataCacheTtlMs_);
        metadataCache_ = new HashMap<>();
      }
    } else {
      LOG.info("Metadata cache not enabled by configuration.");
      metadataCache_ = null;
    }
    perQueryTableNameCache_ = new BoundedCache<>(QUERY_CACHE_SIZE);
    perQueryViewNameCache_ = new BoundedCache<>(QUERY_CACHE_SIZE);
    perQueryTablesCache_ = new BoundedCache<>(QUERY_CACHE_SIZE);
  }

  //
  // List APIs
  //

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    LOG.info("listSchemaNames()");
    List<String> results = new ArrayList<String>();
    MetadataCache cache = getOrCreateCache(session);
    try {
      List<Database> dbs;
      if (cache != null) {
        LOG.debug("Querying cache for listSchemaNames()");
        dbs = cache.getDatabases(() -> { return config.getPlanner(session); });
      } else {
        try (RecordServicePlannerClient planner = config.getPlanner(session)) {
          dbs = planner.getDatabases();
        }
      }
      for (Database db: dbs) {
        results.add(db.toString());
      }
    } catch (RecordServiceException | IOException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to list schemas.\n" + e.getMessage(), e);
    }
    return results;
  }

  @Override
  public List<SchemaTableName> listTables(
      ConnectorSession session, Optional<String> schemaName) {
    LOG.info("listTables(): schema = " + schemaName);
    final String key = session.getQueryId() + schemaName;
    try {
      return perQueryTableNameCache_.get(key, () -> {
        return listTablesOrViews(
            session, schemaName, !disableExternalViews_ ? ListType.TABLE : null);
      });
    } catch (RecordServiceException | IOException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to list tables.\n" + e.getMessage(), e);
    }
  }

  @Override
  public List<SchemaTableName> listViews(ConnectorSession session,
      Optional<String> schemaName) {
    LOG.info("listViews(): schema = " + schemaName);
    if (disableExternalViews_) return new ArrayList<>();
    final String key = session.getQueryId() + schemaName;
    try {
      return perQueryViewNameCache_.get(key, () -> {
        return listTablesOrViews(session, schemaName, ListType.VIEW);
      });
    } catch (RecordServiceException | IOException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to list views.\n" + e.getMessage(), e);
    }
  }

  /**
   * When clients like sqlworkbench list all tables in all schemas,
   * will restrict the count of schemas to MAX_SCHEMAS and list of tables in
   * each schema to MAX_TABLES_PER_SCHEMA, to manage load on ODAS from a single UI client
   * TODO: make this configurable
   */
  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
      ConnectorSession session, SchemaTablePrefix prefix) {
    // TODO: this is not right. Not sure why prefix is implemented this way.
    LOG.info("listTableColumns: " + prefix.getSchema() + " " + prefix.getTable());
    boolean isAllSchemas = false;

    List<String> schemas = new ArrayList<String>();
    if (!prefix.getSchema().isPresent()) {
      isAllSchemas = true;
      schemas = listSchemaNames(session);
      if (schemas != null && schemas.size() > MAX_SCHEMAS) {
        LOG.warn("Too many schemas retrieved: " + schemas.size() + ". Limiting to " +
                    MAX_SCHEMAS);
        schemas = schemas.subList(0, MAX_SCHEMAS);
      }
    } else {
      schemas.add(prefix.getSchema().get());
    }

    RecordServiceTableHandle handle;
    Map<SchemaTableName, List<ColumnMetadata>> builder = new HashMap<SchemaTableName,
         List<ColumnMetadata>>();
    for (String schema: schemas) {
      List<String> tables = new ArrayList<String>();
      if (!prefix.getTable().isPresent()) {
        tables = getTableNames(listTables(session, Optional.of(schema)));
        if (isAllSchemas && tables != null && tables.size() > MAX_TABLES_PER_SCHEMA) {
          LOG.warn("Too many schemas retrieved. Limiting number of tables in schema '" +
                      schema + "' to " + MAX_TABLES_PER_SCHEMA);
          tables = tables.subList(0, MAX_TABLES_PER_SCHEMA);
        }
      } else {
        tables.add(prefix.getTable().get());
      }

      for (String table: tables) {
        SchemaTableName tbl = new SchemaTableName(schema, table);
        handle = (RecordServiceTableHandle) getTableHandle(session, tbl);
        // When allSchemas is requested, often from end clients like
        // sqlworkbench connecting to EMR presto, then failure of bad tables
        // which is usually a very small percent, should not prevent loading
        // the good ones. Log errors for admin to catch.
        try {
          builder.put(tbl, getSchema(handle, session));
        } catch (TrinoException e) {
          LOG.error("bad table found: " + schema + ":" + table + ". Error: ");
          e.printStackTrace();
          continue;
        }
      }
    }
    return builder;
  }

  //
  // Create/Drop APIs
  //

  @Override
  public void createView(ConnectorSession session, SchemaTableName name,
      ConnectorViewDefinition viewMetadata, boolean replace) {
    LOG.info("createView(): view = " + viewMetadata);

    // We have to convert the original view types into Okera types
    // and create a column definition list.
    List<String> columnDefs = Lists.newArrayList();
    for (ViewColumn columnMetadata : viewMetadata.getColumns()) {
      columnDefs.add(String.format(
          "`%s` %s",
          columnMetadata.getName(),
          toOkeraTypeString(typeManager.getType(columnMetadata.getType()))));
    }

    try {
      String originalSql = viewMetadata.getOriginalSql();

      // The view definition can contain an arbitrary set of different types
      // of quotes, and since we embed it in a DDL statement, we need to
      // escape these. `escapeJavaScript` is the closest to the logic we
      // need without needing to write our own escaper.
      // We unescape these when we read the views in `getViews`.
      String escapedSql = StringEscapeUtils.escapeJavaScript(originalSql);

      // We will create a DDL of the following form:
      //    CREATE EXTERNAL VIEW <db>.<view name> (
      //      ... col definitions ...
      //    ) SKIP_ANALYSIS USING VIEW DATA AS "<view definition statement>"
      // Note that we use the "USING VIEW DATA AS" variant since we do not
      // want ODAS to parse this view definition at all, in case it contains
      // syntax ODAS does not support.
      String qualifiedViewName = String.format("`%s`.`%s`",
          name.getSchemaName(), name.getTableName());
      String viewDefTmpl = "CREATE EXTERNAL VIEW %s (%s) " +
          "SKIP_ANALYSIS USING VIEW DATA AS \"%s\"";
      String viewDef = String.format(
          viewDefTmpl,
          qualifiedViewName,
          String.join(",", columnDefs),
          escapedSql
      );

      LOG.info("Creating view with the following DDL:\n" + viewDef);
      try (RecordServicePlannerClient planner = config.getPlanner(session)) {
        if (replace) {
          LOG.info("Dropping view as `REPLACE` was specified: " + qualifiedViewName);
          planner.executeDdl("DROP VIEW IF EXISTS " + qualifiedViewName);
        }

        planner.executeDdl(viewDef);
      }
    } catch (IOException e) {
      LOG.error(e);
      String message = "Error parsing view data: " + e.getMessage();
      throw new TrinoException(GENERIC_INTERNAL_ERROR, message, e);
    } catch (RecordServiceException e) {
      LOG.error(e);
      String message = "Error creating view: " + e.message + ": " + e.detail;
      throw new TrinoException(GENERIC_INTERNAL_ERROR, message, e);
    }
  }

  @Override
  public void dropView(ConnectorSession session, SchemaTableName viewName) {
    LOG.info("dropView(): view = " + viewName);
    try {
      String qualifiedViewName = String.format("`%s`.`%s`",
          viewName.getSchemaName(),
          viewName.getTableName());

      try (RecordServicePlannerClient planner = config.getPlanner(session)) {
        planner.executeDdl("DROP VIEW IF EXISTS " + qualifiedViewName);
      }
    } catch (IOException e) {
      LOG.error(e);
      String message = "Error parsing view data: " + e.getMessage();
      throw new TrinoException(GENERIC_INTERNAL_ERROR, message, e);
    } catch (RecordServiceException e) {
      LOG.error(e);
      String message = "Error dropping view: " + e.message + ": " + e.detail;
      throw new TrinoException(GENERIC_INTERNAL_ERROR, message, e);
    }
  }

  //
  // Get APIs
  //

  @Override
  public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName) {
    // TODO: make this more efficient
    Map<SchemaTableName, ConnectorViewDefinition> views = getViews(session, Optional.of(viewName.getSchemaName()));
    ConnectorViewDefinition view = views.get(viewName);
    if (view == null) {
      return Optional.empty();
    }

    return Optional.of(view);
  }

  /**
   * Gets the view data for views that are in a schema.
   */
  @Override
  public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session,
      Optional<String> schemaName) {
    LOG.info("Calling getViews: " + schemaName);
    Map<SchemaTableName, ConnectorViewDefinition> result = new HashMap<>();
    if (disableExternalViews_)  {
      return result;
    }
    if (!schemaName.isPresent()) {
      // This may be a valid case to allow. We do not know the calling pattern here.
      throw new TrinoException(GENERIC_INTERNAL_ERROR, "schemaName must be set");
    }

    try (RecordServicePlannerClient planner = config.getPlanner(session)) {
      final String key = session.getQueryId() + schemaName.get();
      List<Table> tbls = null;
      Map<String,Table> tableCache = perQueryTablesCache_.get(key);
      if (tableCache != null) {
        tbls = Lists.newArrayList(tableCache.values());
      } else {
        tbls = planner.getDatasets(new Database(schemaName.get()), "*");

        // Store the table map back into the cache, since we fetched it.
        tableCache = new HashMap<>();
        for (Table tbl : tbls) {
          tableCache.put(tbl.name(), tbl);
        }
        perQueryTablesCache_.put(key, tableCache);
      }
      for (Table tbl: tbls) {
        if (ParseUtil.nullOrEmpty(tbl.viewString())) continue;
        final SchemaTableName name = new SchemaTableName(schemaName.get(), tbl.name());
        String catalog = module == null ? OKERA_CATALOG : module.getCatalog();
        String sql = normalizeSql(tbl.viewString());
        List<ViewColumn> columns = new ArrayList<>();
        for (Schema.ColumnDesc col: tbl.schema().cols) {
          columns.add(new ViewColumn(col.name, toTrinoType(col).getTypeId()));
        }
        result.put(name, new ConnectorViewDefinition(
            sql,
            Optional.ofNullable(catalog),
            schemaName,
            columns,
            Optional.ofNullable(tbl.description()),
            Optional.empty(),
            true));
      }
      return result;
    } catch (RecordServiceException | IOException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to get view.\n" + e.getMessage(), e);
    }
  }

  @Override
  public ConnectorTableHandle getTableHandle(
      ConnectorSession session, SchemaTableName tableName) {
    return new RecordServiceTableHandle(
        tableName, TupleDomain.all(), Optional.empty(), false);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle table) {
    RecordServiceTableHandle handle = (RecordServiceTableHandle) table;
    LOG.info("getTableMetadata: " + handle);
    return new ConnectorTableMetadata(
        handle.getSchemaTableName(), getSchema(handle, session));
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
      ConnectorTableHandle table) {
    RecordServiceTableHandle handle = (RecordServiceTableHandle) table;
    LOG.info("getColumnHandles: " + handle);
    List<ColumnMetadata> schema = getSchema(handle, session);
    Map<String, ColumnHandle> results = new HashMap<String, ColumnHandle>();
    for (int i = 0; i < schema.size(); i++) {
      ColumnMetadata md = schema.get(i);
      results.put(md.getName(),
          new RecordServiceColumnHandle(md.getName(), md.getType(), md.getComment(), i));
    }
    return results;
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session,
      ConnectorTableHandle table, ColumnHandle col) {
    LOG.info("Calling getColumnMetadata: " + table);
    RecordServiceColumnHandle handle = (RecordServiceColumnHandle)col;
    return handle.metadata();
  }

  //
  // Private Functions
  //

  /**
   * Gets or creates the metadata cache object. Returns null if the cache
   * should not be used.
   */
  private MetadataCache getOrCreateCache(ConnectorSession session) {
    if (metadataCache_ == null) return null;
    if (session.getUser() == null) return null;
    synchronized (metadataCache_) {
      if (!metadataCache_.containsKey(session.getUser())) {
        metadataCache_.put(session.getUser(), new MetadataCache(metadataCacheTtlMs_));
      }
      return metadataCache_.get(session.getUser());
    }
  }

  // This is a common function that's used to load the stats from
  // the Table structure that came back from the planner.
  private TableStatistics getTableStatisticsFromTableMetadata(
      ConnectorSession session,
      Table table, RecordServiceTableHandle handle)
      throws RecordServiceException, IOException {
    final SchemaTableName name = handle.getSchemaTableName();
    LOG.debug("Calling getTableStatisticsFromTableMetadata: " + name);
    if (table == null) {
      return TableStatistics.empty();
    }
    Table.Stats stats = table.getStats(config.getStatsMode(session));
    if (stats == null || stats.rowCount <= 0) {
      return TableStatistics.empty();
    }

    TableStatistics.Builder builder = new TableStatistics.Builder();
    builder.setRowCount(Estimate.of(stats.rowCount));
    if (handle.getSchema() != null) {
      for (int i = 0; i < handle.getSchema().size(); i++) {
        ColumnMetadata md = handle.getSchema().get(i);
        ColumnHandle columnHandle = new RecordServiceColumnHandle(
            md.getName(), md.getType(), md.getComment(), i);
        ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
        columnStatistics.setNullsFraction(Estimate.of(0));
        columnStatistics.setDistinctValuesCount(Estimate.of(0));
        if (stats.totalByteSize > 0) {
          columnStatistics.setDataSize(Estimate.of(stats.totalByteSize));
        }
        builder.setColumnStatistics(columnHandle, columnStatistics.build());
      }
    }
    return builder.build();
  }

  enum ListType {
    TABLE,
    VIEW,
  }

  // List either tables or views or both (if type is null)
  private List<SchemaTableName> listTablesOrViews(ConnectorSession session,
      Optional<String> schemaName, ListType type)
      throws IOException, RecordServiceException {
    List<SchemaTableName> results = new ArrayList<SchemaTableName>();
    if (INFORMATION_SCHEMA.equalsIgnoreCase(schemaName.orElse(null))) {
      // `information_schema` is a special Presto internal construct that Okera is not
      // aware of, so we should not try and ask Okera for it
      return results;
    }

    MetadataCache cache = getOrCreateCache(session);
    List<String> schemas = new ArrayList<String>();
    if (!schemaName.isPresent()) {
      // null schemaName input is generally an indicator to return all schema
      schemas = listSchemaNames(session);
    } else {
      if (cache != null) {
        // Optimize case where cache is set and it is a single schema.
        List<Table> tbls = cache.getDatasetNames(
            () -> { return config.getPlanner(session); },
            new Database(schemaName.get()));
        for (Table tbl: tbls) {
          final boolean isView = !ParseUtil.nullOrEmpty(tbl.viewString());
          if (type != null && type == ListType.VIEW && !isView) continue;
          if (type != null && type == ListType.TABLE && isView) continue;
          results.add(new SchemaTableName(schemaName.get(), tbl.name()));
        }
        return results;
      }
      schemas.add(schemaName.get());
    }

    try (RecordServicePlannerClient planner = config.getPlanner(session)) {
      for (String schema: schemas) {
        List<Table> tbls = null;

        if (cache != null) {
          tbls = cache.getDatasetNames(() -> { return planner; }, new Database(schema));
        } else if (disableExternalViews_) {
          // If we are not enabling external views, everything is a table and we can
          // speed this up.
          tbls = planner.getDatasetNames(new Database(schema), null);
        } else {
          final String key = session.getQueryId() + schema;
          Map<String,Table> tableCache = perQueryTablesCache_.get(key);
          if (tableCache != null) {
            tbls = Lists.newArrayList(tableCache.values());
          } else {
            tbls = planner.getDatasets(new Database(schema), null);

            // Store the table map back into the cache, since we fetched it.
            tableCache = new HashMap<>();
            for (Table tbl : tbls) {
              tableCache.put(tbl.name(), tbl);
            }
            perQueryTablesCache_.put(key, tableCache);
          }
        }

        for (Table tbl: tbls) {
          final boolean isView = !ParseUtil.nullOrEmpty(tbl.viewString());
          if (type != null && type == ListType.VIEW && !isView) continue;
          if (type != null && type == ListType.TABLE && isView) continue;
          results.add(new SchemaTableName(schema, tbl.name()));
        }
      }
      return results;
    }
  }

  private String normalizeSql(String hiveQl) {
    // We need to unescape any quotes, and the JavaScript dialect
    // is the closest to this without writing our own unescaper.
    return StringEscapeUtils.unescapeJavaScript(hiveQl.replace('`', '"'));
  }

  private List<String> getTableNames(List<SchemaTableName> listTables) {
    List<String> tables = new ArrayList<String>();
    for (SchemaTableName stn: listTables) {
      tables.add(stn.getTableName());
    }
    return tables;
  }

  /**
   * Returns the Okera type string from a Trino type.
   */
  private String toOkeraTypeString(Type type) {
    if (BooleanType.BOOLEAN.equals(type)) {
      return "BOOLEAN";
    }
    if (BigintType.BIGINT.equals(type)) {
      return "BIGINT";
    }
    if (IntegerType.INTEGER.equals(type)) {
      return "INT";
    }
    if (SmallintType.SMALLINT.equals(type)) {
      return "SMALLINT";
    }
    if (TinyintType.TINYINT.equals(type)) {
      return "TINYINT";
    }
    if (RealType.REAL.equals(type)) {
      return "FLOAT";
    }
    if (DoubleType.DOUBLE.equals(type)) {
      return "DOUBLE";
    }
    if (type instanceof VarcharType) {
      // TODO: should we convert better to VARCHAR(N) ?
      return "STRING";
    }
    if (type instanceof CharType) {
      CharType charType = (CharType) type;
      int charLength = charType.getLength();
      return "CHAR(" + charLength + ")";
    }
    if (VarbinaryType.VARBINARY.equals(type)) {
      return "BINARY";
    }
    if (DateType.DATE.equals(type)) {
      return "DATE";
    }
    if (TimestampType.TIMESTAMP_MILLIS.equals(type)) { // TODO: What about the other TIMESTAMP_XXXXX?
      return "TIMESTAMP_NANOS";
    }
    if (TimestampType.TIMESTAMP_MILLIS.getTypeId().equals(type)) { // TODO: What about the other TIMESTAMP_XXXXX?
      return "TIMESTAMP_NANOS";
    }
    if (TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.getTypeId().equals(type)) { // TODO: What about the other TIMESTAMP_XXXXX?
      return "TIMESTAMP_NANOS";
    }
    if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) type;
      return "DECIMAL(" +
          decimalType.getPrecision() + ", " + decimalType.getScale() + ")";
    }
    if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
      return "ARRAY<" + toOkeraTypeString(type.getTypeParameters().get(0)) + ">";
    }
    if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
      String keyType = toOkeraTypeString(type.getTypeParameters().get(0));
      String valueType = toOkeraTypeString(type.getTypeParameters().get(1));
      return "MAP<" + keyType + "," + valueType + ">";
    }
    if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
      ImmutableList.Builder<String> contents = ImmutableList.builder();
      int index = 0;
      for (TypeSignatureParameter parameter : type.getTypeSignature().getParameters()) {
        if (!parameter.isNamedTypeSignature()) {
          throw new IllegalArgumentException(
          String.format(
            "Expected all parameters to be named type, but got %s",
            parameter));
        }
        NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
        if (!namedTypeSignature.getName().isPresent()) {
          throw new TrinoException(
            NOT_SUPPORTED,
            String.format(
              "Anonymous row type is not supported in Okera: %s",
              type));
        }
        String fieldName = namedTypeSignature.getName().get();
        String typeName = toOkeraTypeString(type.getTypeParameters().get(index++));
        contents.add(fieldName + ": " + typeName);
      }
      return "STRUCT<" + String.join(",", contents.build()) + ">";
    }
    throw new TrinoException(
      NOT_SUPPORTED,
      String.format("Unsupported Okera type: %s", type));
  }

  /**
   * Returns the trino type from a recordservice type.
   */
  private Type toTrinoType(Schema.ColumnDesc col) {
    Schema.TypeDesc type = col.type;
    switch (type.typeId) {
      case BOOLEAN: return BooleanType.BOOLEAN;
      case BIGINT: return BigintType.BIGINT;
      case BINARY: return VarcharType.createUnboundedVarcharType();
      case CHAR: return VarcharType.createVarcharType(type.len);
      case DATE: return DateType.DATE;
      case DECIMAL: return DecimalType.createDecimalType(type.precision, type.scale);
      case DOUBLE: return DoubleType.DOUBLE;
      case FLOAT: return DoubleType.DOUBLE;
      case INT: return IntegerType.INTEGER;
      case SMALLINT: return IntegerType.INTEGER;
      case STRING: return VarcharType.createUnboundedVarcharType();
      case TIMESTAMP_NANOS: return TimestampType.TIMESTAMP_NANOS; // TODO: What about the other TIMESTAMP_XXXXX?
      case TINYINT: return IntegerType.INTEGER;
      case VARCHAR: return VarcharType.createVarcharType(type.len);
      case RECORD: {
        ImmutableList.Builder<TypeSignatureParameter> builder = ImmutableList.builder();
        for (Schema.ColumnDesc field: col.fields) {
          TypeSignature t = toTrinoType(field).getTypeSignature();
          String name = field.name.toLowerCase();
          builder.add(TypeSignatureParameter.namedField(name, t));
        }
        return typeManager.getType(new TypeSignature(
            StandardTypes.ROW, builder.build()));
      }
      case ARRAY: {
        TypeSignature listSignature = TypeSignature.arrayType(
            toTrinoType(col.fields.get(0)).getTypeSignature());
        return typeManager.getType(listSignature);
      }
      case MAP: {
        TypeSignature listSignature = TypeSignature.mapType(
            toTrinoType(col.fields.get(0)).getTypeSignature(),
            toTrinoType(col.fields.get(1)).getTypeSignature());
        return typeManager.getType(listSignature);
      }
      case UNSUPPORTED:
        throw new RuntimeException("Unsupported type: " + type);
    }
    throw new RuntimeException("Unsupported type: " + type);
  }

  /**
   * Gets the schema for table.
   */
  private List<ColumnMetadata> getSchema(
      RecordServiceTableHandle table, ConnectorSession session) {
    LOG.debug("getSchema(): " + table);
    if (table.getSchema() != null) return table.getSchema();

    final SchemaTableName name = table.getSchemaTableName();
    Schema schema = null;
    TableStatistics stats = null;

    try (RecordServicePlannerClient planner = config.getPlanner(session)) {
      // Beyond the schema, we will also load the table statistics for
      // this table, to avoid needing to do a separate call (since we are
      // already loading the full table metadata).
      Table okeraTable = null;
      final String key = session.getQueryId() + name.getSchemaName();
      Map<String, Table> tableCache = perQueryTablesCache_.get(key);
      if (tableCache != null && tableCache.containsKey(name.getTableName())) {
        okeraTable = tableCache.get(name.getTableName());
      } else {
        okeraTable = planner.getTable(
            new Database(name.getSchemaName()), name.getTableName());
      }

      schema = okeraTable.schema();
      stats = getTableStatisticsFromTableMetadata(session, okeraTable, table);
    } catch (RecordServiceException | IOException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to get table schema.\n" + e.getMessage(), e);
    }

    List<ColumnMetadata> cols = new ArrayList<ColumnMetadata>();
    if (schema.nestedCols != null) {
      for (Schema.ColumnDesc col: schema.nestedCols) {
        if (!col.hasAccess) continue;
        cols.add(ColumnMetadata.builder()
            .setName(col.name)
            .setType(toTrinoType(col))
            .setComment(Optional.of(col.comment))
            .setHidden(false)
            .build());
      }
    } else {
      for (Schema.ColumnDesc col: schema.cols) {
        if (!col.hasAccess) continue;
        cols.add(ColumnMetadata.builder()
        .setName(col.name)
        .setType(toTrinoType(col))
        .setComment(Optional.of(col.comment))
        .setHidden(false)
        .build());
      }
    }
    table.setSchema(cols);
    table.setStats(stats);
    return table.getSchema();
  }
}
