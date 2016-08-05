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

import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class KuduTableProperties
{
    public static final String RANGE_COLUMNS = "range_columns";
    public static final String NUM_REPLICAS = "replicas";
    public static final String KEY_COLUMNS = "key_columns";
    public static final String HASH_COLUMNS = "hash_columns";
    public static final String BUCKETS = "buckets";
    public static final String SEED = "seed";
    private final List<PropertyMetadata<?>> tableProperties;

    public KuduTableProperties(TypeManager typeManager)
    {
        tableProperties = ImmutableList.of(
                listProperty(
                        KEY_COLUMNS,
                        "Key columns for kudu.",
                        typeManager),
                listProperty(
                        RANGE_COLUMNS,
                        "Range columns for kudu.",
                        typeManager),
                listProperty(
                        HASH_COLUMNS,
                        "Hash columns for kudu.",
                        typeManager),
                integerSessionProperty(
                        NUM_REPLICAS,
                        "Number of replicas.",
                        3,
                        false),
                integerSessionProperty(
                        BUCKETS,
                        "Number of buckets for hash partitions.",
                        null,
                        false),
                integerSessionProperty(
                        SEED,
                        "Seed for hash partitions.",
                        0,
                        false));
    }

    private PropertyMetadata<?> listProperty(String name, String desc, TypeManager typeManager)
    {
        return new PropertyMetadata<>(
                name,
                desc,
                typeManager.getType(
                        parseTypeSignature(
                                "array(varchar)")),
                List.class,
                ImmutableList.of(),
                false,
                value -> ImmutableList.copyOf(
                        (Collection<?>) value),
                value -> value);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getRangePartitionColumns(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(
                RANGE_COLUMNS);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getHashColumns(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(
                HASH_COLUMNS);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getKeyColumns(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(
                KEY_COLUMNS);
    }

    public static int getNumReplicas(Map<String, Object> tableProperties)
    {
        return (int) tableProperties.get(
                NUM_REPLICAS);
    }

    public static Integer getBuckets(Map<String, Object> tableProperties)
    {
        return (Integer) tableProperties.get(
                BUCKETS);
    }

    public static int getSeed(Map<String, Object> tableProperties)
    {
        return (int) tableProperties.get(
                SEED);
    }
}
