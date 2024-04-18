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

/** An enumeration indicating the clone operator. */
public enum CloneType {
    LatestSnapshot(
            "LatestSnapshot", "Clone the latest snapshot form source table to target table."),

    SpecificSnapshot(
            "SpecificSnapshot", "Clone a specific snapshot form source table to target table."),

    FromTimestamp(
            "FromTimestamp",
            "Snapshot around timestamp (If there is no snapshot earlier than this time,"
                    + " the earliest snapshot will be chosen) will be cloned into the target table."),

    Tag("Tag", "Clone a tag form source table to target table."),

    Table(
            "Table",
            "Clone all snapshots and tags form source table to target table"
                    + " before the command starts, this is mainly used for data migration.");

    private final String value;
    private final String description;

    CloneType(String value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String toString() {
        return value + " : " + description;
    }
}
