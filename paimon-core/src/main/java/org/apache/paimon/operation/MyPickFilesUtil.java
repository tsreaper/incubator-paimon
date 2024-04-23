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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Utils. */
public class MyPickFilesUtil {

    private static final int READ_FILE_RETRY_NUM = 3;
    private static final int READ_FILE_RETRY_INTERVAL = 5;

    public static List<Path> getUsedFilesForSnapshot(
            Snapshot snapshot,
            SnapshotManager snapshotManager,
            FileStorePathFactory pathFactory,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler) {
        List<Path> files = new ArrayList<>();
        files.add(snapshotManager.snapshotPath(snapshot.id()));
        files.addAll(
                getUsedFilesInternal(
                        snapshot,
                        pathFactory,
                        manifestList,
                        manifestFile,
                        schemaManager,
                        indexFileHandler));
        return files;
    }

    private static List<Path> getUsedFilesInternal(
            Snapshot snapshot,
            FileStorePathFactory pathFactory,
            ManifestList manifestList,
            ManifestFile manifestFile,
            SchemaManager schemaManager,
            IndexFileHandler indexFileHandler) {
        List<Path> files = new ArrayList<>();
        addManifestList(files, snapshot, pathFactory);

        try {
            // try to read manifests
            List<ManifestFileMeta> manifestFileMetas =
                    retryReadingFiles(
                            () -> readAllManifestsWithIOException(snapshot, manifestList));
            if (manifestFileMetas == null) {
                return Collections.emptyList();
            }
            List<String> manifestFileName =
                    manifestFileMetas.stream()
                            .map(ManifestFileMeta::fileName)
                            .collect(Collectors.toList());
            files.addAll(
                    manifestFileName.stream()
                            .map(pathFactory::toManifestFilePath)
                            .collect(Collectors.toList()));

            // try to read data files
            List<Path> dataFiles =
                    retryReadingDataFiles(manifestFileName, manifestFile, pathFactory);
            if (dataFiles == null) {
                return Collections.emptyList();
            }
            files.addAll(dataFiles);

            // try to read index files
            String indexManifest = snapshot.indexManifest();
            if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
                files.add(pathFactory.indexManifestFileFactory().toPath(indexManifest));

                List<IndexManifestEntry> indexManifestEntries =
                        retryReadingFiles(
                                () -> indexFileHandler.readManifestWithIOException(indexManifest));
                if (indexManifestEntries == null) {
                    return Collections.emptyList();
                }

                indexManifestEntries.stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(indexFileHandler::filePath)
                        .forEach(files::add);
            }

            // add statistic file
            if (snapshot.statistics() != null) {
                files.add(pathFactory.statsFileFactory().toPath(snapshot.statistics()));
            }

            // add schema file
            for (long id : schemaManager.listAllIds()) {
                files.add(schemaManager.toSchemaPath(id));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    private static void addManifestList(
            List<Path> used, Snapshot snapshot, FileStorePathFactory pathFactory) {
        used.add(pathFactory.toManifestListPath(snapshot.baseManifestList()));
        used.add(pathFactory.toManifestListPath(snapshot.deltaManifestList()));
        String changelogManifestList = snapshot.changelogManifestList();
        if (changelogManifestList != null) {
            used.add(pathFactory.toManifestListPath(changelogManifestList));
        }
    }

    private static List<ManifestFileMeta> readAllManifestsWithIOException(
            Snapshot snapshot, ManifestList manifestList) throws IOException {
        List<ManifestFileMeta> result = new ArrayList<>();

        result.addAll(manifestList.readWithIOException(snapshot.baseManifestList()));
        result.addAll(manifestList.readWithIOException(snapshot.deltaManifestList()));

        String changelogManifestList = snapshot.changelogManifestList();
        if (changelogManifestList != null) {
            result.addAll(manifestList.readWithIOException(changelogManifestList));
        }

        return result;
    }

    @Nullable
    private static List<Path> retryReadingDataFiles(
            List<String> manifestNames, ManifestFile manifestFile, FileStorePathFactory pathFactory)
            throws IOException {
        List<Path> dataFiles = new ArrayList<>();
        for (String manifestName : manifestNames) {
            List<ManifestEntry> manifestEntries =
                    retryReadingFiles(() -> manifestFile.readWithIOException(manifestName));
            if (manifestEntries == null) {
                return null;
            }

            manifestEntries.forEach(
                    e -> {
                        DataFilePathFactory factory =
                                pathFactory.createDataFilePathFactory(e.partition(), e.bucket());
                        dataFiles.add(factory.toPath(e.file().fileName()));
                        for (String f : e.file().extraFiles()) {
                            dataFiles.add(factory.toPath(f));
                        }
                    });
        }
        return dataFiles;
    }

    @Nullable
    private static <T> T retryReadingFiles(OrphanFilesClean.ReaderWithIOException<T> reader)
            throws IOException {
        int retryNumber = 0;
        IOException caught = null;
        while (retryNumber++ < READ_FILE_RETRY_NUM) {
            try {
                return reader.read();
            } catch (FileNotFoundException e) {
                return null;
            } catch (IOException e) {
                caught = e;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_FILE_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        throw caught;
    }
}
