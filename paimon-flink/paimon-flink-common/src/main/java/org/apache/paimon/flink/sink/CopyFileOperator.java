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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.source.CloneFileInfo;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.Map;

/** A Operator to copy files. */
public class CopyFileOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo> {

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private AbstractCatalog sourceCatalog;
    private AbstractCatalog targetCatalog;

    public CopyFileOperator(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        super();
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
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
        sourceCatalog =
                (AbstractCatalog)
                        FlinkCatalogFactory.createPaimonCatalog(
                                Options.fromMap(sourceCatalogConfig));
        targetCatalog =
                (AbstractCatalog)
                        FlinkCatalogFactory.createPaimonCatalog(
                                Options.fromMap(targetCatalogConfig));
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        CloneFileInfo cloneFileInfo = streamRecord.getValue();

        FileIO sourceTableFileIO = sourceCatalog.fileIO();
        FileIO targetTableFileIO = targetCatalog.fileIO();
        Path sourceTableRootPath =
                sourceCatalog.getDataTableLocation(
                        Identifier.fromString(cloneFileInfo.sourceIdentifier()));
        Path targetTableRootPath =
                targetCatalog.getDataTableLocation(
                        Identifier.fromString(cloneFileInfo.targetIdentifier()));

        CopyFileUtils.copyFile(
                cloneFileInfo,
                sourceTableFileIO,
                targetTableFileIO,
                sourceTableRootPath,
                targetTableRootPath);
        output.collect(streamRecord);
    }
}
