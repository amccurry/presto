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
package kudu;

import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduScanner.KuduScannerBuilder;
import org.kududb.client.KuduTable;
import org.kududb.client.LocatedTablet;
import org.kududb.client.Partition;
import org.kududb.client.RowResultIterator;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ReadDataSplitKudu
{
    private ReadDataSplitKudu()
    {}

    public static void main(String[] args)
            throws Exception
    {
//        List<ColumnSchema> columns = new ArrayList<>(2);
//        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
//        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
//        List<String> rangeKeys = new ArrayList<>();
//        rangeKeys.add("key");
//        Schema schema = new Schema(columns);
        String tableName = "test_kudu_2";
        try (KuduClient client = new KuduClient.KuduClientBuilder("localhost").build()) {
            KuduTable table = client.openTable(tableName);
            List<LocatedTablet> tabletsLocations = table.getTabletsLocations(Long.MAX_VALUE);
            for (LocatedTablet locatedTablet : tabletsLocations) {
                Partition partition = locatedTablet.getPartition();
                System.out.println(" Start ============ " + partition);
                byte[] partitionKeyStart = partition.getPartitionKeyStart();
                byte[] partitionKeyEnd = partition.getPartitionKeyEnd();
                KuduScannerBuilder scannerBuilder = client.newScannerBuilder(table);
                scannerBuilder.lowerBoundPartitionKeyRaw(partitionKeyStart);
                scannerBuilder.exclusiveUpperBoundPartitionKeyRaw(partitionKeyEnd);
                KuduScanner scanner = scannerBuilder.build();
                AtomicLong rowCount = new AtomicLong();
                while (scanner.hasMoreRows()) {
                    RowResultIterator iterator = scanner.nextRows();
                    iterator.forEach(r -> {
//                        System.out.println(r.getInt("key") + "=>" + r.getString("value"));
                        rowCount.incrementAndGet();
                    });
                }
                scanner.close();
                System.out.println(" End ============ (" + rowCount + ") " + partition);
            }
        }
    }
}
