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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.action.CloneType;
import org.apache.paimon.flink.source.CloneFileInfo;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Operator. */
public class SnapshotHintOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo>, BoundedOneInput {

    private final Map<String, String> targetCatalogConfig;
    private final CloneType cloneType;

    private Catalog targetCatalog;
    private Set<String> identifiers;

    public SnapshotHintOperator(Map<String, String> targetCatalogConfig, CloneType cloneType) {
        this.targetCatalogConfig = targetCatalogConfig;
        this.cloneType = cloneType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        targetCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(targetCatalogConfig));
        identifiers = new HashSet<>();
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        String identifier = streamRecord.getValue().targetIdentifier();
        identifiers.add(identifier);
    }

    @Override
    public void endInput() throws Exception {
        for (String identifier : identifiers) {
            FileStoreTable targetTable =
                    (FileStoreTable) targetCatalog.getTable(Identifier.fromString(identifier));
            commitSnapshotHintInTargetTable(targetTable.snapshotManager());
        }
    }

    private void commitSnapshotHintInTargetTable(SnapshotManager targetTableSnapshotManager)
            throws IOException {
        switch (cloneType) {
            case LatestSnapshot:
            case SpecificSnapshot:
            case FromTimestamp:
                Preconditions.checkState(
                        1 == targetTableSnapshotManager.snapshotCount(),
                        "snapshot count is not equal to 1 "
                                + "when cloneType is LATEST_SNAPSHOT / SPECIFIC_SNAPSHOT / FROM_TIMESTAMP.");
                long snapshotId = targetTableSnapshotManager.safelyGetAllSnapshots().get(0).id();
                targetTableSnapshotManager.commitEarliestHint(snapshotId);
                targetTableSnapshotManager.commitLatestHint(snapshotId);
                break;

            case Table:
                Long latestSnapshotId = targetTableSnapshotManager.latestSnapshotId();
                if (latestSnapshotId != null) {
                    targetTableSnapshotManager.commitLatestHint(latestSnapshotId);
                }

                Long earliestSnapshotId = targetTableSnapshotManager.earliestSnapshotId();
                if (earliestSnapshotId != null) {
                    targetTableSnapshotManager.commitEarliestHint(earliestSnapshotId);
                }
                break;

            case Tag:
            default:
                throw new UnsupportedOperationException("Unknown cloneType : " + cloneType);
        }
    }
}
