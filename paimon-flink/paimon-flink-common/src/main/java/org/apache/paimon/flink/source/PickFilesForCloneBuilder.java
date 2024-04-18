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

package org.apache.paimon.flink.source;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.action.CloneType;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.PickFilesUtil;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.SnapshotManager.SNAPSHOT_PREFIX;
import static org.apache.paimon.utils.TagManager.TAG_PREFIX;

/** Source builder to build a Flink batch job. This is for clone jobs. */
public class PickFilesForCloneBuilder {

    private final CloneType cloneType;
    private final StreamExecutionEnvironment env;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final long snapshotId;
    private final String tagName;
    private final long timestamp;
    private final ManifestList manifestList;
    private final ManifestFile manifestFile;
    private final IndexFileHandler indexFileHandler;
    private final SchemaManager schemaManager;
    private final Path sourceTableRoot;
    private final int partitionNum;
    private final FileIO fileIO;

    public PickFilesForCloneBuilder(
            StreamExecutionEnvironment env,
            FileStoreTable sourceTable,
            CloneType cloneType,
            long snapshotId,
            String tagName,
            long timestamp) {
        this.env = env;
        this.cloneType = cloneType;
        this.snapshotId = snapshotId;
        this.tagName = tagName;
        this.timestamp = timestamp;
        this.schemaManager = sourceTable.schemaManager();
        FileStore<?> store = sourceTable.store();
        this.snapshotManager = store.snapshotManager();
        this.tagManager = store.newTagManager();
        this.manifestList = store.manifestListFactory().create();
        this.manifestFile = store.manifestFileFactory().create();
        this.indexFileHandler = store.newIndexFileHandler();
        this.sourceTableRoot = sourceTable.location();
        this.partitionNum = sourceTable.partitionKeys().size();
        this.fileIO = sourceTable.fileIO();
    }

    /**
     * Find the files to be copied in different cloneType.
     *
     * @return all cloneFileInfo to be copied
     */
    private List<CloneFileInfo> findCloneFilePaths() {
        Map<String, Pair<Path, Long>> tableAllFilesExcludeTableRoot =
                getTableAllFilesExcludeTableRoot();

        switch (cloneType) {
            case LatestSnapshot:
                Snapshot latestSnapshot = snapshotManager.latestSnapshot();
                checkNotNull(latestSnapshot, "Error, table has no snapshot.");
                return getFilePathsForSnapshotOrTag(
                        PickFilesUtil.getUsedFilesForSnapshot(
                                latestSnapshot,
                                snapshotManager,
                                manifestList,
                                manifestFile,
                                schemaManager,
                                indexFileHandler),
                        tableAllFilesExcludeTableRoot,
                        fileName -> fileName.startsWith(SNAPSHOT_PREFIX));

            case SpecificSnapshot:
                return getFilePathsForSnapshotOrTag(
                        PickFilesUtil.getUsedFilesForSnapshot(
                                snapshotManager.snapshot(snapshotId),
                                snapshotManager,
                                manifestList,
                                manifestFile,
                                schemaManager,
                                indexFileHandler),
                        tableAllFilesExcludeTableRoot,
                        fileName -> fileName.startsWith(SNAPSHOT_PREFIX));

            case FromTimestamp:
                Snapshot snapshot = snapshotManager.earlierOrEqualTimeMills(timestamp);
                checkNotNull(snapshot, "Error, table has no snapshot.");
                return getFilePathsForSnapshotOrTag(
                        PickFilesUtil.getUsedFilesForSnapshot(
                                snapshot,
                                snapshotManager,
                                manifestList,
                                manifestFile,
                                schemaManager,
                                indexFileHandler),
                        tableAllFilesExcludeTableRoot,
                        fileName -> fileName.startsWith(SNAPSHOT_PREFIX));

            case Tag:
                checkArgument(tagManager.tagExists(tagName), "Tag name '%s' not exists.", tagName);
                return getFilePathsForSnapshotOrTag(
                        PickFilesUtil.getUsedFilesForTag(
                                tagManager.taggedSnapshot(tagName),
                                tagManager,
                                tagName,
                                manifestList,
                                manifestFile,
                                schemaManager,
                                indexFileHandler),
                        tableAllFilesExcludeTableRoot,
                        fileName -> fileName.startsWith(TAG_PREFIX));

            case Table:
                return tableAllFilesExcludeTableRoot.entrySet().stream()
                        .map(
                                entry ->
                                        new CloneFileInfo(
                                                entry.getValue().getLeft(),
                                                entry.getValue().getRight(),
                                                entry.getKey().startsWith(SNAPSHOT_PREFIX)
                                                        || entry.getKey().startsWith(TAG_PREFIX)))
                        .collect(Collectors.toList());
            default:
                throw new UnsupportedOperationException("Unknown cloneType : " + cloneType);
        }
    }

    /**
     * @param fileNames all fileNames of a snapshot or tag
     * @param tableAllFiles all files of a table. path is the relative path
     * @param snapshotOrTagFile filter by snapshot or tag file
     * @return
     */
    private List<CloneFileInfo> getFilePathsForSnapshotOrTag(
            List<String> fileNames,
            Map<String, Pair<Path, Long>> tableAllFiles,
            Predicate<String> snapshotOrTagFile) {
        List<CloneFileInfo> cloneFileInfos = new ArrayList<>();

        for (String fileName : fileNames) {
            Pair<Path, Long> filePathAndSize = tableAllFiles.get(fileName);
            checkNotNull(
                    filePathAndSize,
                    "Error this is a bug, please report. fileName is "
                            + fileName
                            + ", tableAllFiles is : "
                            + tableAllFiles);
            cloneFileInfos.add(
                    new CloneFileInfo(
                            filePathAndSize.getKey(),
                            filePathAndSize.getValue(),
                            snapshotOrTagFile.test(fileName)));
        }
        return cloneFileInfos;
    }

    /**
     * Get all files of a table.
     *
     * @return fileName -> (relative path, file size)
     */
    private Map<String, Pair<Path, Long>> getTableAllFilesExcludeTableRoot() {
        Map<String, Pair<Path, Long>> tableAllFiles =
                PickFilesUtil.getTableAllFiles(sourceTableRoot, partitionNum, fileIO);

        return tableAllFiles.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                        Pair.of(
                                                getPathExcludeTableRoot(e.getValue().getKey()),
                                                e.getValue().getRight())));
    }

    /**
     * cut table root path of absolutePath.
     *
     * @param absolutePath
     * @return the relative path
     */
    private Path getPathExcludeTableRoot(Path absolutePath) {
        String fileAbsolutePath = absolutePath.toUri().getPath();
        String sourceTableRootPath = sourceTableRoot.toString();

        checkState(
                fileAbsolutePath.startsWith(sourceTableRootPath),
                "This is a bug, please report. fileAbsolutePath is : "
                        + fileAbsolutePath
                        + ", sourceTableRootPath is : "
                        + sourceTableRootPath);

        return new Path(fileAbsolutePath.substring(sourceTableRootPath.length()));
    }

    public DataStream<CloneFileInfo> build() {
        checkNotNull(env, "StreamExecutionEnvironment should not be null.");

        return env.fromCollection(findCloneFilePaths()).forceNonParallel().rebalance();
    }
}
