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

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.TypeManager;
import org.kududb.client.KuduClient;

import java.util.List;

public class KuduConnector
        implements Connector
{
    private final String connectorId;
    private final KuduClient client;
    private final List<PropertyMetadata<?>> tableProperties;

    public KuduConnector(String connectorId, KuduClient client, TypeManager typeManager)
    {
        this.connectorId = connectorId;
        this.client = client;
        tableProperties = new KuduTableProperties(typeManager).getTableProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return KuduConnectorTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return new KuduConnectorMetadata(connectorId, client);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new KuduConnectorSplitManager(client);
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return new KuduConnectorRecordSetProvider(client);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public void shutdown()
    {
        try {
            client.shutdown();
        }
        catch (Exception e) {
            throw KuduExceptionUtil.error("Shutdown error.", e);
        }
    }
}
