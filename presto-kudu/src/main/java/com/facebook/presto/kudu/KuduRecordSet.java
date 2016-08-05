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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.kududb.client.KuduClient;

import java.util.List;
import static com.facebook.presto.kudu.Types.checkType;

public class KuduRecordSet
        implements RecordSet
{
    private final KuduClient client;
    private final KuduConnectorSplit split;
    private final List<Type> types;
    private final List<KuduColumnHandle> columns;

    public KuduRecordSet(KuduClient client, KuduConnectorSplit kuduConnectorSplit, List<? extends ColumnHandle> columns)
    {
        this.client = client;
        this.split = kuduConnectorSplit;
        Builder<Type> typeBuilder = ImmutableList.builder();
        Builder<KuduColumnHandle> columnBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columns) {
            KuduColumnHandle kuduColumnHandle = checkType(columnHandle, KuduColumnHandle.class, "columnHandle");
            columnBuilder.add(kuduColumnHandle);
            typeBuilder.add(kuduColumnHandle.getType());
        }
        types = typeBuilder.build();
        this.columns = columnBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KuduRecordCursor(client, columns, split);
    }
}
