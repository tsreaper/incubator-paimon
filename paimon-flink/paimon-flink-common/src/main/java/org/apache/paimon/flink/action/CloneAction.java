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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.CheckCloneResultAndCopySnapshotOperator;
import org.apache.paimon.flink.sink.CopyFileOperator;
import org.apache.paimon.flink.source.CloneFileInfo;
import org.apache.paimon.flink.source.CloneFileInfoTypeInfo;
import org.apache.paimon.flink.source.PickFilesForCloneBuilder;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Snapshot/Tag/Table clone action for Flink. */
public class CloneAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(CloneAction.class);

    private final CloneType cloneType;
    private final int parallelism;
    private final long snapshotId;
    private final String tagName;
    private final long timestamp;

    private final FileStoreTable sourceFileStoreTable;
    private FileStoreTable targetFileStoreTable;

    private final Path sourceTableRootPath;
    private final Path targetTableRootPath;

    private final boolean copyInDifferentCluster;

    public CloneAction(
            String sourceWarehouse,
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            int parallelism,
            CloneType cloneType,
            long snapshotId,
            String tagName,
            long timestamp) {
        super(sourceWarehouse, sourceDatabase, sourceTableName, sourceCatalogConfig);
        this.cloneType = cloneType;
        this.parallelism = parallelism;
        this.snapshotId = snapshotId;
        this.tagName = tagName;
        this.timestamp = timestamp;
        this.sourceFileStoreTable = checkTableAndCast(table);

        initTargetTableAndCleanDir(
                targetWarehouse,
                targetDatabase,
                targetTableName,
                targetCatalogConfig,
                sourceFileStoreTable.schema());

        this.sourceTableRootPath = sourceFileStoreTable.location();
        this.targetTableRootPath = targetFileStoreTable.location();
        this.copyInDifferentCluster = judgeCopyInDifferentCluster();
        LOG.info("copyInDifferentCluster is {}. ", copyInDifferentCluster);
    }

    private void initTargetTableAndCleanDir(
            String targetWarehouse,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            TableSchema sourceSchema) {
        Options targetTableCatalogOptions = Options.fromMap(targetCatalogConfig);
        targetTableCatalogOptions.set(CatalogOptions.WAREHOUSE, targetWarehouse);
        Catalog targetCatalog = FlinkCatalogFactory.createPaimonCatalog(targetTableCatalogOptions);
        try {
            if (!targetCatalog.databaseExists(targetDatabase)) {
                targetCatalog.createDatabase(targetDatabase, true);
            }
            Identifier targetIdentifier = new Identifier(targetDatabase, targetTableName);
            Table targetTable;
            if (targetCatalog.tableExists(targetIdentifier)) {
                targetTable = targetCatalog.getTable(targetIdentifier);
                this.targetFileStoreTable = checkTableAndCast(targetTable);
                targetFileStoreTable
                        .fileIO()
                        .deleteDirectoryQuietly(targetFileStoreTable.location());
            } else {
                targetCatalog.createTable(
                        targetIdentifier, Schema.fromTableSchema(sourceSchema), false);
                targetTable = targetCatalog.getTable(targetIdentifier);
                this.targetFileStoreTable = checkTableAndCast(targetTable);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean judgeCopyInDifferentCluster() {
        FileIO sourceTableFileIO = sourceFileStoreTable.fileIO();
        FileIO targetTableFileIO = targetFileStoreTable.fileIO();

        URI sourceTableRootUri;
        URI targetTableRootUri;
        try {
            sourceTableRootUri =
                    sourceTableFileIO.getFileStatus(sourceTableRootPath).getPath().toUri();
            targetTableRootUri =
                    targetTableFileIO.getFileStatus(targetTableRootPath).getPath().toUri();
        } catch (IOException e) {
            throw new RuntimeException("get file status error.");
        }

        String sourceTableUriSchema = sourceTableRootUri.getScheme();
        checkNotNull(sourceTableUriSchema, "Schema of sourceTableRootUri should not be null.");

        String targetTableUriSchema = targetTableRootUri.getScheme();
        checkNotNull(targetTableUriSchema, "Schema of targetTableRootUri should not be null.");

        if (!sourceTableUriSchema.equals(targetTableUriSchema)) {
            return true;
        }

        String sourceTableUriAuthority = sourceTableRootUri.getAuthority();
        String targetTableUriAuthority = targetTableRootUri.getAuthority();
        if (sourceTableUriAuthority == null && targetTableUriAuthority == null) {
            return false;
        }
        if (sourceTableUriAuthority == null || targetTableUriAuthority == null) {
            return true;
        }

        LOG.info(
                "sourceTableRootPath is {}, targetTableRootPath is {}, "
                        + "sourceTableRootUri is {}, targetTableRootUri is {},"
                        + "sourceTableUriSchema is {}, targetTableUriSchema is {},"
                        + " sourceTableUriAuthority is {}, targetTableUriAuthority is {}.",
                sourceTableRootPath,
                targetTableRootPath,
                sourceTableRootUri,
                targetTableRootUri,
                sourceTableUriSchema,
                targetTableUriSchema,
                sourceTableUriAuthority,
                targetTableUriAuthority);

        return !sourceTableUriAuthority.equals(targetTableUriAuthority);
    }

    private FileStoreTable checkTableAndCast(Table table) {
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "table type error. The table type is '%s'.",
                            table.getClass().getName()));
        }
        return (FileStoreTable) table;
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    @Override
    public void build() {
        checkFlinkParameters();
        buildCloneFlinkJob(env);
    }

    private void checkFlinkParameters() {
        // only support batch clone yet
        if (env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                != RuntimeExecutionMode.BATCH) {
            LOG.warn(
                    "Clone only support batch mode yet. "
                            + "Please add -Dexecution.runtime-mode=BATCH."
                            + " The action this time will shift to batch mode forcely.");
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
    }

    private void buildCloneFlinkJob(StreamExecutionEnvironment env) {
        DataStream<CloneFileInfo> pickFilesForClone =
                new PickFilesForCloneBuilder(
                                env,
                                sourceFileStoreTable,
                                cloneType,
                                snapshotId,
                                tagName,
                                timestamp)
                        .build();

        SingleOutputStreamOperator<CloneFileInfo> copyFiles =
                pickFilesForClone
                        .transform(
                                "Copy Files",
                                new CloneFileInfoTypeInfo(),
                                new CopyFileOperator(
                                        sourceFileStoreTable,
                                        targetFileStoreTable,
                                        copyInDifferentCluster,
                                        sourceTableRootPath,
                                        targetTableRootPath))
                        .setParallelism(parallelism);

        SingleOutputStreamOperator<CloneFileInfo> checkCloneResult =
                copyFiles
                        .transform(
                                "Check Copy Files Result",
                                new CloneFileInfoTypeInfo(),
                                new CheckCloneResultAndCopySnapshotOperator(
                                        sourceFileStoreTable,
                                        targetFileStoreTable,
                                        copyInDifferentCluster,
                                        sourceTableRootPath,
                                        targetTableRootPath,
                                        cloneType))
                        .setParallelism(1)
                        .setMaxParallelism(1);

        checkCloneResult.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Clone job");
    }
}
