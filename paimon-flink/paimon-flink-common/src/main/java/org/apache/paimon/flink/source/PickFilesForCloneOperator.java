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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.action.CloneType;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.MyPickFilesUtil;
import org.apache.paimon.operation.PickFilesUtil;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Operator. */
public class PickFilesForCloneOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<Tuple2<String, String>, CloneFileInfo> {

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;
    private final CloneType cloneType;
    private final long snapshotId;
    private final String tagName;
    private final long timestamp;

    private Catalog sourceCatalog;
    private Catalog targetCatalog;

    public PickFilesForCloneOperator(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            CloneType cloneType,
            long snapshotId,
            String tagName,
            long timestamp) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
        this.cloneType = cloneType;
        this.snapshotId = snapshotId;
        this.tagName = tagName;
        this.timestamp = timestamp;
    }

    @Override
    public void open() throws Exception {
        super.open();
        sourceCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
        targetCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(targetCatalogConfig));
    }

    @Override
    public void processElement(StreamRecord<Tuple2<String, String>> streamRecord) throws Exception {
        try {
            processElementImpl(streamRecord);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to clone from table "
                            + streamRecord.getValue().f0
                            + " to table "
                            + streamRecord.getValue().f1,
                    e);
        }
    }

    private void processElementImpl(StreamRecord<Tuple2<String, String>> streamRecord)
            throws Exception {
        String sourceIdentifierStr = streamRecord.getValue().f0;
        Identifier sourceIdentifier = Identifier.fromString(sourceIdentifierStr);
        String targetIdentifierStr = streamRecord.getValue().f1;
        Identifier targetIdentifier = Identifier.fromString(targetIdentifierStr);

        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        FileStore<?> store = sourceTable.store();
        SnapshotManager snapshotManager = store.snapshotManager();
        TagManager tagManager = store.newTagManager();
        ManifestList manifestList = store.manifestListFactory().create();
        ManifestFile manifestFile = store.manifestFileFactory().create();
        IndexFileHandler indexFileHandler = store.newIndexFileHandler();
        SchemaManager schemaManager = sourceTable.schemaManager();
        int partitionNum = sourceTable.partitionKeys().size();

        targetCatalog.createDatabase(targetIdentifier.getDatabaseName(), true);
        targetCatalog.createTable(
                targetIdentifier, Schema.fromTableSchema(sourceTable.schema()), true);
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);

        FileIO sourceFileIO = sourceTable.fileIO();
        FileIO targetFileIO = targetTable.fileIO();
        Path sourceTableRoot = sourceTable.location();
        Path targetTableRoot = targetTable.location();

        Supplier<Map<String, Pair<Path, Long>>> getTableAllFiles =
                () -> {
                    Map<String, Pair<Path, Long>> tableAllFiles =
                            PickFilesUtil.getTableAllFiles(
                                    sourceTableRoot, partitionNum, sourceFileIO);

                    return tableAllFiles.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            e ->
                                                    Pair.of(
                                                            getPathExcludeTableRoot(
                                                                    e.getValue().getKey(),
                                                                    sourceTableRoot),
                                                            e.getValue().getRight())));
                };

        List<CloneFileInfo> result;
        List<String> usedFiles;
        Map<String, Pair<Path, Long>> tableAllFiles;

        switch (cloneType) {
            case LatestSnapshot:
                Snapshot latestSnapshot = snapshotManager.latestSnapshot();
                checkNotNull(latestSnapshot, "Error, table has no snapshot.");
                result =
                        toCloneFileInfos(
                                MyPickFilesUtil.getUsedFilesForSnapshot(
                                        latestSnapshot,
                                        snapshotManager,
                                        sourceTable.store().pathFactory(),
                                        manifestList,
                                        manifestFile,
                                        schemaManager,
                                        indexFileHandler),
                                sourceFileIO,
                                targetFileIO,
                                sourceTableRoot,
                                targetTableRoot,
                                sourceIdentifierStr,
                                targetIdentifierStr);
                break;

            case SpecificSnapshot:
                result =
                        toCloneFileInfos(
                                MyPickFilesUtil.getUsedFilesForSnapshot(
                                        snapshotManager.snapshot(snapshotId),
                                        snapshotManager,
                                        sourceTable.store().pathFactory(),
                                        manifestList,
                                        manifestFile,
                                        schemaManager,
                                        indexFileHandler),
                                sourceFileIO,
                                targetFileIO,
                                sourceTableRoot,
                                targetTableRoot,
                                sourceIdentifierStr,
                                targetIdentifierStr);
                break;

            case FromTimestamp:
                Snapshot snapshot = snapshotManager.earlierOrEqualTimeMills(timestamp);
                checkNotNull(snapshot, "Error, table has no snapshot.");
                result =
                        toCloneFileInfos(
                                MyPickFilesUtil.getUsedFilesForSnapshot(
                                        snapshot,
                                        snapshotManager,
                                        sourceTable.store().pathFactory(),
                                        manifestList,
                                        manifestFile,
                                        schemaManager,
                                        indexFileHandler),
                                sourceFileIO,
                                targetFileIO,
                                sourceTableRoot,
                                targetTableRoot,
                                sourceIdentifierStr,
                                targetIdentifierStr);
                break;

            case Tag:
                checkArgument(tagManager.tagExists(tagName), "Tag name '%s' not exists.", tagName);
                usedFiles =
                        PickFilesUtil.getUsedFilesForTag(
                                tagManager.taggedSnapshot(tagName),
                                tagManager,
                                tagName,
                                manifestList,
                                manifestFile,
                                schemaManager,
                                indexFileHandler);
                tableAllFiles = getTableAllFiles.get();
                result =
                        getFilePathsForSnapshotOrTag(
                                usedFiles, tableAllFiles, sourceIdentifierStr, targetIdentifierStr);
                break;

            case Table:
                result =
                        getTableAllFiles.get().entrySet().stream()
                                .map(
                                        entry ->
                                                new CloneFileInfo(
                                                        entry.getValue().getLeft(),
                                                        sourceIdentifierStr,
                                                        targetIdentifierStr))
                                .collect(Collectors.toList());
                break;

            default:
                throw new UnsupportedOperationException("Unknown cloneType : " + cloneType);
        }

        for (CloneFileInfo info : result) {
            if (info.getFilePathExcludeTableRoot().toString().endsWith("snapshot/EARLIEST")
                    || info.getFilePathExcludeTableRoot().toString().endsWith("snapshot/LATEST")) {
                continue;
            }
            output.collect(new StreamRecord<>(info));
        }
    }

    private List<CloneFileInfo> toCloneFileInfos(
            List<Path> files,
            FileIO sourceFileIO,
            FileIO targetFileIO,
            Path sourceTableRoot,
            Path targetTableRoot,
            String sourceIdentifier,
            String targetIdentifier)
            throws Exception {
        List<CloneFileInfo> result = new ArrayList<>();
        for (Path file : files) {
            Path relativePath = getPathExcludeTableRoot(file, sourceTableRoot);
            Path sourceAbsolutePath = new Path(sourceTableRoot, relativePath);
            Path targetAbsolutePath = new Path(targetTableRoot, relativePath);
            if (targetFileIO.exists(targetAbsolutePath)
                    && targetFileIO.getFileSize(targetAbsolutePath)
                            == sourceFileIO.getFileSize(sourceAbsolutePath)) {
                continue;
            }
            result.add(new CloneFileInfo(relativePath, sourceIdentifier, targetIdentifier));
        }
        return result;
    }

    private List<CloneFileInfo> getFilePathsForSnapshotOrTag(
            List<String> fileNames,
            Map<String, Pair<Path, Long>> tableAllFiles,
            String sourceIdentifier,
            String targetIdentifier) {
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
                            filePathAndSize.getKey(), sourceIdentifier, targetIdentifier));
        }
        return cloneFileInfos;
    }

    private Path getPathExcludeTableRoot(Path absolutePath, Path sourceTableRoot) {
        String fileAbsolutePath = absolutePath.toUri().toString();
        String sourceTableRootPath = sourceTableRoot.toString();

        checkState(
                fileAbsolutePath.startsWith(sourceTableRootPath),
                "This is a bug, please report. fileAbsolutePath is : "
                        + fileAbsolutePath
                        + ", sourceTableRootPath is : "
                        + sourceTableRootPath);

        return new Path(fileAbsolutePath.substring(sourceTableRootPath.length()));
    }
}
