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

package org.apache.paimon.iceberg;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

import static org.apache.paimon.options.ConfigOptions.key;

/** Config options for Paimon Iceberg compatibility. */
public class IcebergOptions {

    public static final ConfigOption<StorageType> METADATA_ICEBERG_STORAGE =
            key("metadata.iceberg.storage")
                    .enumType(StorageType.class)
                    .noDefaultValue()
                    .withDescription(
                            "When set, produce Iceberg metadata after a snapshot is committed, "
                                    + "so that Iceberg readers can read Paimon's raw files.\n"
                                    + "PER_TABLE: Store Iceberg metadata with each table.\n"
                                    + "ICEBERG_WAREHOUSE: Store Iceberg metadata in a separate directory. "
                                    + "This directory can be specified as the Iceberg warehouse directory.");

    public static final ConfigOption<Integer> COMPACT_MIN_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.min.file-num")
                    .intType()
                    .defaultValue(10);

    public static final ConfigOption<Integer> COMPACT_MAX_FILE_NUM =
            ConfigOptions.key("metadata.iceberg.compaction.max.file-num")
                    .intType()
                    .defaultValue(50);

    /** Where to store Iceberg metadata. */
    public enum StorageType {
        // Store Iceberg metadata with each table.
        PER_TABLE,
        // Store Iceberg metadata in a separate directory.
        // This directory can be specified as the Iceberg warehouse directory.
        ICEBERG_WAREHOUSE
    }
}