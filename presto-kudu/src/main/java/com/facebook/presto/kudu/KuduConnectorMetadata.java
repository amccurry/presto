/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kudu;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.FloatType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import org.kududb.ColumnSchema;
import org.kududb.ColumnSchema.ColumnSchemaBuilder;
import org.kududb.Schema;
import org.kududb.client.AlterTableOptions;
import org.kududb.client.CreateTableOptions;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;
import org.kududb.client.ListTablesResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.facebook.presto.kudu.KuduTableProperties.BUCKETS;
import static com.facebook.presto.kudu.KuduTableProperties.HASH_COLUMNS;
import static com.facebook.presto.kudu.KuduTableProperties.RANGE_COLUMNS;
import static com.facebook.presto.kudu.Types.checkType;

public class KuduConnectorMetadata
        implements ConnectorMetadata
{
    private final KuduClient client;
    private final String connectorId;

    public KuduConnectorMetadata(String connectorId, KuduClient client)
    {
        this.connectorId = connectorId;
        this.client = client;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return Arrays.asList(connectorId);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            ListTablesResponse tablesList = client.getTablesList();
            List<String> list = tablesList.getTablesList();
            if (!list.contains(tableName.getTableName())) {
                return null;
            }
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
        return new KuduConnectorTableHandle(connectorId, tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        KuduConnectorTableHandle tableHandle = checkType(table, KuduConnectorTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new KuduConnectorTableLayoutHandle(tableHandle.getConnectorId(), tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduConnectorTableHandle kuduConnectorTableHandle = checkType(tableHandle, KuduConnectorTableHandle.class, "tableHandle");
        Map<String, ColumnHandle> columnHandles = new TreeMap<>(getColumnHandles(session, tableHandle));
        Builder<ColumnMetadata> builder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles.values()) {
            builder.add(getColumnMetadata(session, tableHandle, columnHandle));
        }
        return new ConnectorTableMetadata(kuduConnectorTableHandle.getSchemaTableName(), builder.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null || schemaNameOrNull.equals(connectorId)) {
            try {
                ListTablesResponse tablesList = client.getTablesList();
                List<String> list = tablesList.getTablesList();
                Builder<SchemaTableName> builder = ImmutableList.builder();
                for (String table : list) {
                    builder.add(new SchemaTableName(connectorId, table));
                }
                return builder.build();
            }
            catch (Exception e) {
                throw KuduExceptionUtil.error("Error while trying to get table list.", e);
            }
        }
        else {
            return ImmutableList.of();
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduConnectorTableHandle kuduConnectorTableHandle = checkType(tableHandle, KuduConnectorTableHandle.class, "tableHandle");
        try {
            KuduTable table = client.openTable(kuduConnectorTableHandle.getSchemaTableName()
                                                                       .getTableName());
            Schema schema = table.getSchema();
            return getColumnHandles(schema);
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }

    private Map<String, ColumnHandle> getColumnHandles(Schema schema)
    {
        List<ColumnSchema> columns = schema.getColumns();
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnSchema columnSchema : columns) {
            String name = columnSchema.getName();
            org.kududb.Type type = columnSchema.getType();
            builder.put(name, toColumnHandle(name, type));
        }
        return builder.build();
    }

    private ColumnHandle toColumnHandle(String name, org.kududb.Type type)
    {
        return new KuduColumnHandle(name, toPrestoType(type));
    }

    private Type toPrestoType(org.kududb.Type type)
    {
        switch (type) {
            case BINARY:
                return VarbinaryType.VARBINARY;
            case BOOL:
                return BooleanType.BOOLEAN;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case FLOAT:
                return FloatType.FLOAT;
            case INT16:
                return SmallintType.SMALLINT;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case INT8:
                return TinyintType.TINYINT;
            case STRING:
                return VarcharType.VARCHAR;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            default:
                throw KuduExceptionUtil.error("Kudu type [] does not map to a Presto type.");
        }
    }

    private org.kududb.Type toKuduType(Type type)
    {
        if (type instanceof VarbinaryType) {
            return org.kududb.Type.BINARY;
        }
        else if (type instanceof BooleanType) {
            return org.kududb.Type.BOOL;
        }
        else if (type instanceof DoubleType) {
            return org.kududb.Type.DOUBLE;
        }
        else if (type instanceof FloatType) {
            return org.kududb.Type.FLOAT;
        }
        else if (type instanceof SmallintType) {
            return org.kududb.Type.INT16;
        }
        else if (type instanceof IntegerType) {
            return org.kududb.Type.INT32;
        }
        else if (type instanceof BigintType) {
            return org.kududb.Type.INT64;
        }
        else if (type instanceof TinyintType) {
            return org.kududb.Type.INT8;
        }
        else if (type instanceof VarcharType) {
            return org.kududb.Type.STRING;
        }
        else if (type instanceof TimestampType) {
            return org.kududb.Type.TIMESTAMP;
        }
        else {
            throw KuduExceptionUtil.error("Kudu type [" + type + "] does not map to a Presto type.");
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, KuduConnectorTableHandle.class, "tableHandle");
        KuduColumnHandle kuduColumnHandle = checkType(columnHandle, KuduColumnHandle.class, "columnHandle");
        Type type = kuduColumnHandle.getType();
        return new ColumnMetadata(kuduColumnHandle.getName(), type);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> mapBuilder = ImmutableMap.builder();
        List<String> listSchemaNames = listSchemaNames(session);
        for (String schema : listSchemaNames) {
            List<SchemaTableName> listTables = listTables(session, schema);
            for (SchemaTableName schemaTableName : listTables) {
                if (prefix.matches(schemaTableName)) {
                    Builder<ColumnMetadata> builder = ImmutableList.builder();
                    ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName);
                    Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
                    for (Entry<String, ColumnHandle> e : columnHandles.entrySet()) {
                        builder.add(getColumnMetadata(session, tableHandle, e.getValue()));
                    }
                    mapBuilder.put(schemaTableName, builder.build());
                }
            }
        }
        return mapBuilder.build();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        Map<String, Object> properties = tableMetadata.getProperties();
        int numReplicas = KuduTableProperties.getNumReplicas(properties);
        List<String> keyColumns = KuduTableProperties.getKeyColumns(properties);
        List<String> rangeColumns = KuduTableProperties.getRangePartitionColumns(properties);
        List<String> hashColumns = KuduTableProperties.getHashColumns(properties);
        List<ColumnSchema> columns = getColumnSchemas(keyColumns, tableMetadata.getColumns());
        Schema schema = new Schema(columns);
        String tableName = schemaTableName.getTableName();
        CreateTableOptions options = new CreateTableOptions();
        options.setNumReplicas(numReplicas);
        if (rangeColumns != null && !rangeColumns.isEmpty()) {
            options.setRangePartitionColumns(rangeColumns);
        }
        else if (hashColumns != null && !hashColumns.isEmpty()) {
            Integer buckets = KuduTableProperties.getBuckets(properties);
            if (buckets == null) {
                throw KuduExceptionUtil.error("When using [" + HASH_COLUMNS + "] the [" + BUCKETS + "] property is required.");
            }
            int seed = KuduTableProperties.getSeed(properties);
            options.addHashPartitions(hashColumns, buckets, seed);
        }
        else {
            throw KuduExceptionUtil.error("Either [" + RANGE_COLUMNS + "] or [" + HASH_COLUMNS + "] needs to be set for table creation.");
        }
        try {
            client.createTable(tableName, schema, options);
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }

    private List<ColumnSchema> getColumnSchemas(List<String> keys, List<ColumnMetadata> columns)
    {
        Builder<ColumnSchema> builder = ImmutableList.builder();
        columns.forEach(cm -> {
            builder.add(convert(keys, cm));
        });
        return builder.build();
    }

    private ColumnSchema convert(List<String> keys, ColumnMetadata columnMetadata)
    {
        String name = columnMetadata.getName();
        Type type = columnMetadata.getType();
        ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, toKuduType(type));
        if (keys.contains(name)) {
            builder.key(true);
        }
        return builder.build();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        KuduConnectorTableHandle kuduConnectorTableHandle = checkType(tableHandle, KuduConnectorTableHandle.class, "tableHandle");
        try {
            String tableName = kuduConnectorTableHandle.getSchemaTableName()
                                                       .getTableName();
            client.alterTable(tableName, addColumnAlterTableOption(column));
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }

    private AlterTableOptions addColumnAlterTableOption(ColumnMetadata column)
    {
        String name = column.getName();
        org.kududb.Type type = toKuduType(column.getType());
        return new AlterTableOptions().addColumn(name, type, null);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        KuduConnectorTableHandle kuduConnectorTableHandle = checkType(tableHandle, KuduConnectorTableHandle.class, "tableHandle");
        KuduColumnHandle kuduColumnHandle = checkType(source, KuduColumnHandle.class, "source");
        try {
            String tableName = kuduConnectorTableHandle.getSchemaTableName()
                                                       .getTableName();
            client.alterTable(tableName, renameColumnAlterTableOption(kuduColumnHandle, target));
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }

    private AlterTableOptions renameColumnAlterTableOption(KuduColumnHandle kuduColumnHandle, String target)
    {
        String name = kuduColumnHandle.getName();
        return new AlterTableOptions().renameColumn(name, target);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduConnectorTableHandle kuduConnectorTableHandle = checkType(tableHandle, KuduConnectorTableHandle.class, "tableHandle");
        try {
            String tableName = kuduConnectorTableHandle.getSchemaTableName()
                                                       .getTableName();
            client.deleteTable(tableName);
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        KuduConnectorTableHandle kuduConnectorTableHandle = checkType(tableHandle, KuduConnectorTableHandle.class, "tableHandle");
        try {
            String tableName = kuduConnectorTableHandle.getSchemaTableName()
                                                       .getTableName();
            client.alterTable(tableName, renameTableAlterTableOption(newTableName.getTableName()));
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }

    private AlterTableOptions renameTableAlterTableOption(String target)
    {
        return new AlterTableOptions().renameTable(target);
    }
//    @Override
//    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
//    {
//        // TODO Auto-generated method stub
//        ConnectorMetadata.super.createView(session, viewName, viewData, replace);
//    }
//
//    @Override
//    public void dropView(ConnectorSession session, SchemaTableName viewName)
//    {
//        // TODO Auto-generated method stub
//        ConnectorMetadata.super.dropView(session, viewName);
//    }
//
//    @Override
//    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
//    {
//        // TODO Auto-generated method stub
//        return ConnectorMetadata.super.listViews(session, schemaNameOrNull);
//    }
//
//    @Override
//    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
//    {
//        // TODO Auto-generated method stub
//        return ConnectorMetadata.super.getViews(session, prefix);
//    }
//
}
