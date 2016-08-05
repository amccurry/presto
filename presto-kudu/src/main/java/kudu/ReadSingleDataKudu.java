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

import org.kududb.ColumnSchema;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduPredicate.ComparisonOp;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduScanner.KuduScannerBuilder;
import org.kududb.client.KuduTable;
import org.kududb.client.RowResultIterator;

public class ReadSingleDataKudu
{
    private ReadSingleDataKudu()
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
        String tableName = "test_kudu";
        try (KuduClient client = new KuduClient.KuduClientBuilder("localhost").build()) {
            KuduTable table = client.openTable(tableName);
            KuduScannerBuilder scannerBuilder = client.newScannerBuilder(table);
            ColumnSchema columnSchema = table.getSchema().getColumn("key");
            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(columnSchema, ComparisonOp.EQUAL, 111);
            scannerBuilder.addPredicate(predicate);
            KuduScanner scanner = scannerBuilder.build();
            while (scanner.hasMoreRows()) {
                System.out.println("============");
                RowResultIterator iterator = scanner.nextRows();
                iterator.forEach(r -> {
                    System.out.println(r.getInt("key"));
                    System.out.println(r.getString("value"));
                });
            }
            scanner.close();
        }
    }
}
