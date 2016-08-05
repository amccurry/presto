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
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.Upsert;

import java.util.UUID;

public class LoadDataKudu
{
    private LoadDataKudu()
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
            KuduSession session = client.newSession();
            KuduTable table = client.openTable(tableName);
//            AsyncKuduClient asyncClient = table.getAsyncClient();
//            AsyncKuduSession session = asyncClient.newSession();
            for (int i = 0; i < 1000000; i++) {
                if (i % 1000 == 0) {
                    System.out.println(i);
                }
                Upsert upsert = table.newUpsert();
                upsert.getRow().addString("key", Integer.toString(i) + "-" + System.currentTimeMillis());
                upsert.getRow().addString("value", UUID.randomUUID().toString());
                session.apply(upsert);
            }
            session.close();
        }
    }
}
