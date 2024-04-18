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

import org.apache.paimon.fs.Path;

/** The information of copy file. */
public class CloneFileInfo {

    private final Path filePathExcludeTableRoot;
    private final long fileSize;
    private final boolean isSnapshotOrTagFile;

    public CloneFileInfo(
            Path filePathExcludeTableRoot, long fileSize, boolean isSnapshotOrTagFile) {
        this.filePathExcludeTableRoot = filePathExcludeTableRoot;
        this.fileSize = fileSize;
        this.isSnapshotOrTagFile = isSnapshotOrTagFile;
    }

    public Path getFilePathExcludeTableRoot() {
        return filePathExcludeTableRoot;
    }

    public long getFileSize() {
        return fileSize;
    }

    public boolean isSnapshotOrTagFile() {
        return isSnapshotOrTagFile;
    }
}
