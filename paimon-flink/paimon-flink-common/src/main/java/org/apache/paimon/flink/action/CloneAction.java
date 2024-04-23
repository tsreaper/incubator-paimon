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

package org.apache.paimon.flink.action;

import org.apache.paimon.flink.sink.CopyFileOperator;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.SnapshotHintChannelComputer;
import org.apache.paimon.flink.sink.SnapshotHintOperator;
import org.apache.paimon.flink.source.CloneFileInfo;
import org.apache.paimon.flink.source.CloneFileInfoTypeInfo;
import org.apache.paimon.flink.source.CloneSourceBuilder;
import org.apache.paimon.flink.source.PickFilesForCloneOperator;
import org.apache.paimon.options.CatalogOptions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Map;

/** Snapshot/Tag/Table clone action for Flink. */
public class CloneAction extends ActionBase {

    private final CloneType cloneType;
    private final int parallelism;
    private final long snapshotId;
    private final String tagName;
    private final long timestamp;

    private final Map<String, String> sourceCatalogConfig;
    private final String database;
    private final String tableName;

    private final Map<String, String> targetCatalogConfig;
    private final String targetDatabase;
    private final String targetTableName;

    public CloneAction(
            Map<String, String> sourceCatalogConfig,
            String warehouse,
            String database,
            String tableName,
            Map<String, String> targetCatalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetTableName,
            int parallelism,
            CloneType cloneType,
            long snapshotId,
            String tagName,
            long timestamp) {
        super(warehouse, sourceCatalogConfig);

        this.cloneType = cloneType;
        this.parallelism = parallelism;
        this.snapshotId = snapshotId;
        this.tagName = tagName;
        this.timestamp = timestamp;

        this.sourceCatalogConfig = sourceCatalogConfig;
        sourceCatalogConfig.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        this.database = database;
        this.tableName = tableName;

        this.targetCatalogConfig = targetCatalogConfig;
        targetCatalogConfig.put(CatalogOptions.WAREHOUSE.key(), targetWarehouse);
        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    @Override
    public void build() {
        buildCloneFlinkJob(env);
    }

    private void buildCloneFlinkJob(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, String>> cloneSource =
                new CloneSourceBuilder(
                                env,
                                sourceCatalogConfig,
                                database,
                                tableName,
                                targetDatabase,
                                targetTableName)
                        .build();

        SingleOutputStreamOperator<CloneFileInfo> pickFilesForClone =
                cloneSource
                        .transform(
                                "Pick Files",
                                new CloneFileInfoTypeInfo(),
                                new PickFilesForCloneOperator(
                                        sourceCatalogConfig,
                                        targetCatalogConfig,
                                        cloneType,
                                        snapshotId,
                                        tagName,
                                        timestamp))
                        .setParallelism(1);

        SingleOutputStreamOperator<CloneFileInfo> copyFiles =
                pickFilesForClone
                        .rebalance()
                        .transform(
                                "Copy Files",
                                new CloneFileInfoTypeInfo(),
                                new CopyFileOperator(sourceCatalogConfig, targetCatalogConfig))
                        .setParallelism(parallelism);

        SingleOutputStreamOperator<CloneFileInfo> snapshotHintOperator =
                FlinkStreamPartitioner.partition(
                                copyFiles, new SnapshotHintChannelComputer(), parallelism)
                        .transform(
                                "Recreate Snapshot Hint",
                                new CloneFileInfoTypeInfo(),
                                new SnapshotHintOperator(targetCatalogConfig, cloneType))
                        .setParallelism(parallelism);

        snapshotHintOperator.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Clone job");
    }
}
