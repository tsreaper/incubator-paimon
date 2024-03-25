/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.RowDataPartitionComputer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** {@link MetastoreClient} for Hive tables. */
public class HiveMetastoreClient implements MetastoreClient {

    private final Identifier identifier;
    private final RowDataPartitionComputer partitionComputer;

    private final IMetaStoreClient client;
    private final StorageDescriptor sd;

    private HiveMetastoreClient(
            Identifier identifier,
            Map<String, String> tableOptions,
            RowType partitionType,
            IMetaStoreClient client)
            throws Exception {
        this.identifier = identifier;
        this.partitionComputer =
                new RowDataPartitionComputer(
                        new CoreOptions(tableOptions).partitionDefaultName(),
                        partitionType,
                        partitionType.getFieldNames().toArray(new String[0]));

        this.client = client;
        this.sd = client.getTable(identifier.getDatabaseName(), identifier.getObjectName()).getSd();
    }

    @Override
    public void addPartition(BinaryRow partition) throws Exception {
        addPartition(partitionComputer.generatePartValues(partition));
    }

    @Override
    public void addPartition(LinkedHashMap<String, String> partitionSpec) throws Exception {
        List<String> partitionValues = new ArrayList<>(partitionSpec.values());
        try {
            client.getPartition(
                    identifier.getDatabaseName(), identifier.getObjectName(), partitionValues);
            // do nothing if the partition already exists
        } catch (NoSuchObjectException e) {
            // partition not found, create new partition
            StorageDescriptor newSd = new StorageDescriptor(sd);
            newSd.setLocation(
                    sd.getLocation()
                            + "/"
                            + PartitionPathUtils.generatePartitionPath(partitionSpec));

            Partition hivePartition = new Partition();
            hivePartition.setDbName(identifier.getDatabaseName());
            hivePartition.setTableName(identifier.getObjectName());
            hivePartition.setValues(partitionValues);
            hivePartition.setSd(newSd);
            int currentTime = (int) (System.currentTimeMillis() / 1000);
            hivePartition.setCreateTime(currentTime);
            hivePartition.setLastAccessTime(currentTime);

            client.add_partition(hivePartition);
        }
    }

    @Override
    public void deletePartition(LinkedHashMap<String, String> partitionSpec) throws Exception {
        List<String> partitionValues = new ArrayList<>(partitionSpec.values());
        try {
            client.dropPartition(
                    identifier.getDatabaseName(),
                    identifier.getObjectName(),
                    partitionValues,
                    false);
        } catch (NoSuchObjectException e) {
            // do nothing if the partition not exists
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    /** Factory to create {@link HiveMetastoreClient}. */
    public static class Factory implements MetastoreClient.Factory {

        private static final long serialVersionUID = 1L;

        private final Identifier identifier;
        private final Map<String, String> tableOptions;
        private final RowType partitionType;
        private final SerializableHiveConf hiveConf;
        private final String clientClassName;

        public Factory(
                Identifier identifier,
                Map<String, String> tableOptions,
                RowType partitionType,
                HiveConf hiveConf,
                String clientClassName) {
            this.identifier = identifier;
            this.tableOptions = tableOptions;
            this.partitionType = partitionType;
            this.hiveConf = new SerializableHiveConf(hiveConf);
            this.clientClassName = clientClassName;
        }

        @Override
        public MetastoreClient create() {
            HiveConf conf = hiveConf.conf();
            try {
                return new HiveMetastoreClient(
                        identifier,
                        tableOptions,
                        partitionType,
                        HiveCatalog.createClient(conf, clientClassName));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
