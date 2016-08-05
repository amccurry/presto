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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import org.kududb.client.KuduClient;

import java.util.Map;

public class KuduConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;

    public KuduConnectorFactory(NodeManager nodeManager, TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public String getName()
    {
        return "kudu";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new KuduConnectorHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        String master = config.get(
            "kudu.master");
        KuduClient client = new KuduClient.KuduClientBuilder(master).build();
        return new KuduConnector(connectorId, client, typeManager);
    }
}
