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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduTable;
import org.kududb.client.LocatedTablet;
import org.kududb.client.Partition;

import java.util.List;

import static com.facebook.presto.kudu.Types.checkType;

public class KuduConnectorSplitManager
        implements ConnectorSplitManager
{
    private final KuduClient client;

    public KuduConnectorSplitManager(KuduClient client)
    {
        this.client = client;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        KuduConnectorTableLayoutHandle kuduConnectorTableLayoutHandle = checkType(layout, KuduConnectorTableLayoutHandle.class, "layout");
        KuduConnectorTableHandle tableHandle = kuduConnectorTableLayoutHandle.getTableHandle();
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        String tableName = tableHandle.getSchemaTableName().getTableName();
        try {
            KuduTable table = client.openTable(tableName);
            List<LocatedTablet> tabletsLocations = table.getTabletsLocations(Long.MAX_VALUE);
            for (LocatedTablet locatedTablet : tabletsLocations) {
                Partition partition = locatedTablet.getPartition();
                byte[] partitionKeyStart = partition.getPartitionKeyStart();
                byte[] partitionKeyEnd = partition.getPartitionKeyEnd();
                splits.add(new KuduConnectorSplit(tableName, partitionKeyStart, partitionKeyEnd));
            }
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error(e);
        }
        return new FixedSplitSource(splits.build());
    }
}
