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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.FloatType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduTable;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;

import java.util.List;

public class KuduRecordCursor
        implements RecordCursor
{
    private final List<KuduColumnHandle> columns;
    private final KuduScanner kuduScanner;
    private RowResultIterator rowResultIterator;
    private RowResult rowResult;
    private long readTimeNanos;
    private long completedBytes;

    public KuduRecordCursor(KuduClient client, List<KuduColumnHandle> columns, KuduConnectorSplit split)
    {
        this.columns = columns;
        String name = split.getTableName();
        byte[] partitionKeyStart = split.getPartitionKeyStart();
        byte[] partitionKeyEnd = split.getPartitionKeyEnd();
        try {
            KuduTable table = client.openTable(name);
            kuduScanner = client.newScannerBuilder(table)
                                .setProjectedColumnNames(getColumnNames(columns))
                                .lowerBoundPartitionKeyRaw(partitionKeyStart)
                                .exclusiveUpperBoundPartitionKeyRaw(partitionKeyEnd)
                                .build();
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }

    private List<String> getColumnNames(List<KuduColumnHandle> columns)
    {
        Builder<String> columnNames = ImmutableList.builder();
        columns.forEach(c -> columnNames.add(c.getName()));
        return columnNames.build();
    }

    @Override
    public long getTotalBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public Type getType(int field)
    {
        return columns.get(field)
                      .getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        long start = System.nanoTime();
        try {
            if (rowResultIterator != null) {
                if (rowResultIterator.hasNext()) {
                    rowResult = rowResultIterator.next();
                    return true;
                }
                else {
                    rowResultIterator = null;
                    rowResult = null;
                }
            }
            if (kuduScanner.hasMoreRows()) {
                try {
                    rowResultIterator = kuduScanner.nextRows();
                    return advanceNextPosition();
                }
                catch (Exception e) {
                    throw KuduExceptionUtil.error(e);
                }
            }
            return false;
        }
        finally {
            readTimeNanos += (System.nanoTime() - start);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        KuduColumnHandle kuduColumnHandle = columns.get(field);
        Type type = kuduColumnHandle.getType();
        if (type instanceof BooleanType) {
            completedBytes++;
            return rowResult.getBoolean(field);
        }
        else {
            throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, "Type [" + type + "] of column [" + kuduColumnHandle.getName() + "] cannot be converted to a boolean.");
        }
    }

    @Override
    public long getLong(int field)
    {
        KuduColumnHandle kuduColumnHandle = columns.get(field);
        Type type = kuduColumnHandle.getType();
        if (type instanceof TinyintType) {
            completedBytes++;
            return rowResult.getByte(field);
        }
        else if (type instanceof SmallintType) {
            completedBytes += 2;
            return rowResult.getShort(field);
        }
        else if (type instanceof IntegerType) {
            completedBytes += 4;
            return rowResult.getInt(field);
        }
        else if (type instanceof BigintType) {
            completedBytes += 8;
            return rowResult.getLong(field);
        }
        else {
            throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, "Type [" + type + "] of column [" + kuduColumnHandle.getName() + "] cannot be converted to a long.");
        }
    }

    @Override
    public double getDouble(int field)
    {
        KuduColumnHandle kuduColumnHandle = columns.get(field);
        Type type = kuduColumnHandle.getType();
        if (type instanceof DoubleType) {
            completedBytes += 8;
            return rowResult.getDouble(field);
        }
        else if (type instanceof FloatType) {
            completedBytes += 4;
            return rowResult.getFloat(field);
        }
        else {
            throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, "Type [" + type + "] of column [" + kuduColumnHandle.getName() + "] cannot be converted to a double.");
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        KuduColumnHandle kuduColumnHandle = columns.get(field);
        Type type = kuduColumnHandle.getType();
        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            Slice slice = Slices.wrappedBuffer(rowResult.getBinaryCopy(field));
            completedBytes += slice.length();
            return slice;
        }
        else {
            throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, "Type [" + type + "] of column [" + kuduColumnHandle.getName() + "] cannot be converted to a slice.");
        }
    }

    @Override
    public Object getObject(int field)
    {
        throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, "not impl");
    }

    @Override
    public boolean isNull(int field)
    {
        return rowResult.isNull(field);
    }

    @Override
    public void close()
    {
        try {
            kuduScanner.close();
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
    }
}
