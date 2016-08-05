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
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.CreateTableOptions;
import org.kududb.client.KuduClient;

import java.util.ArrayList;
import java.util.List;

public class CreateBasicKuduTable
{
    private CreateBasicKuduTable()
    {}

    public static void main(String[] args)
            throws Exception
    {
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("key");
        Schema schema = new Schema(columns);
        String tableName = "test_kudu_3";
        KuduClient client = new KuduClient.KuduClientBuilder("localhost").build();
        CreateTableOptions options = new CreateTableOptions().setNumReplicas(1).setRangePartitionColumns(rangeKeys);
        client.createTable(tableName, schema, options);
        client.close();
    }
}
