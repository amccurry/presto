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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KuduConnectorTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String connectorId;
    private final KuduConnectorTableHandle tableHandle;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public KuduConnectorTableLayoutHandle(@JsonProperty("connectorId") String connectorId, @JsonProperty("tableHandle") KuduConnectorTableHandle tableHandle, @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.connectorId = connectorId;
        this.tableHandle = tableHandle;
        this.tupleDomain = tupleDomain;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public KuduConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public String toString()
    {
        return "KuduConnectorTableLayoutHandle [connectorId=" + connectorId + ", tableHandle=" + tableHandle + ", tupleDomain=" + tupleDomain + "]";
    }
}
