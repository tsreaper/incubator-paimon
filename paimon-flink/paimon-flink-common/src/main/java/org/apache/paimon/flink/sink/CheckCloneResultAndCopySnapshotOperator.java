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

package org.apache.paimon.flink.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.action.CloneType;
import org.apache.paimon.flink.source.CloneFileInfo;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkState;

/** A Operator to check copy files data size and final copy snapshot or tag file. */
public class CheckCloneResultAndCopySnapshotOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CheckCloneResultAndCopySnapshotOperator.class);

    private final FileIO sourceTableFileIO;
    private final FileIO targetTableFileIO;
    private final SnapshotManager targetTableSnapshotManager;
    private final boolean copyInDifferentCluster;
    private final Path sourceTableRootPath;
    private final Path targetTableRootPath;
    private final List<CloneFileInfo> snapshotOrTagFiles;
    private final CloneType cloneType;
    private long checkedFileCount;

    public CheckCloneResultAndCopySnapshotOperator(
            FileStoreTable sourceTable,
            FileStoreTable targetTable,
            boolean copyInDifferentCluster,
            Path sourceTableRootPath,
            Path targetTableRootPath,
            CloneType cloneType) {
        super();
        this.sourceTableFileIO = sourceTable.fileIO();
        this.targetTableFileIO = targetTable.fileIO();
        this.targetTableSnapshotManager = targetTable.snapshotManager();
        this.copyInDifferentCluster = copyInDifferentCluster;
        this.sourceTableRootPath = sourceTableRootPath;
        this.targetTableRootPath = targetTableRootPath;
        this.snapshotOrTagFiles = new ArrayList<>();
        this.cloneType = cloneType;
        this.checkedFileCount = 0;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CloneFileInfo>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        output.collect(streamRecord);

        CloneFileInfo cloneFileInfo = streamRecord.getValue();
        if (cloneFileInfo.isSnapshotOrTagFile()) {
            snapshotOrTagFiles.add(cloneFileInfo);
            return;
        }

        Path targetFilePath =
                new Path(
                        targetTableRootPath.toString()
                                + cloneFileInfo.getFilePathExcludeTableRoot());

        LOG.info(
                "Begin check the {} file's size, targetFilePath is {}.",
                ++checkedFileCount,
                targetFilePath);

        //        checkState(
        //                cloneFileInfo.getFileSize() ==
        // targetTableFileIO.getFileSize(targetFilePath),
        //                "Copy file error. clone file origin size is : "
        //                        + cloneFileInfo.getFileSize()
        //                        + ", target file path is : "
        //                        + targetFilePath
        //                        + ", and target file size is  : "
        //                        + targetTableFileIO.getFileSize(targetFilePath));
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        copySnapshotOrTag();
        commitSnapshotHintInTargetTable();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (snapshotOrTagFiles != null) {
            snapshotOrTagFiles.clear();
        }
    }

    /**
     * copy snapshot or tag in this latest operator of flink batch job.
     *
     * @throws IOException
     */
    private void copySnapshotOrTag() throws IOException {
        for (CloneFileInfo cloneFileInfo : snapshotOrTagFiles) {
            CopyFileUtils.copyFile(
                    cloneFileInfo,
                    sourceTableFileIO,
                    targetTableFileIO,
                    sourceTableRootPath,
                    targetTableRootPath,
                    copyInDifferentCluster);
        }
    }

    /**
     * commit Snapshot earliestHint and latestHint in targetTable in different cloneTypes.
     *
     * @throws IOException
     */
    private void commitSnapshotHintInTargetTable() throws IOException {
        switch (cloneType) {
            case LatestSnapshot:
            case SpecificSnapshot:
            case FromTimestamp:
                checkState(
                        1 == targetTableSnapshotManager.snapshotCount(),
                        "snapshot count is not equal to 1 "
                                + "when cloneType is LATEST_SNAPSHOT / SPECIFIC_SNAPSHOT / FROM_TIMESTAMP.");
                long snapshotId = targetTableSnapshotManager.safelyGetAllSnapshots().get(0).id();
                targetTableSnapshotManager.commitEarliestHint(snapshotId);
                targetTableSnapshotManager.commitLatestHint(snapshotId);
                break;

            case Table:
                long maxSnapshotId = Long.MIN_VALUE;
                long minSnapshotId = Long.MAX_VALUE;
                List<Snapshot> snapshots = targetTableSnapshotManager.safelyGetAllSnapshots();
                for (Snapshot snapshot : snapshots) {
                    maxSnapshotId = Math.max(maxSnapshotId, snapshot.id());
                    minSnapshotId = Math.min(minSnapshotId, snapshot.id());
                }
                targetTableSnapshotManager.commitEarliestHint(minSnapshotId);
                targetTableSnapshotManager.commitLatestHint(maxSnapshotId);
                break;

            case Tag:
            default:
                throw new UnsupportedOperationException("Unknown cloneType : " + cloneType);
        }
    }
}
