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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Builder. */
public class CloneSourceBuilder {

    private final StreamExecutionEnvironment env;
    private final Map<String, String> sourceCatalogConfig;
    private final String database;
    private final String tableName;
    private final String targetDatabase;
    private final String targetTableName;

    public CloneSourceBuilder(
            StreamExecutionEnvironment env,
            Map<String, String> sourceCatalogConfig,
            String database,
            String tableName,
            String targetDatabase,
            String targetTableName) {
        this.env = env;
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.database = database;
        this.tableName = tableName;
        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
    }

    public DataStream<Tuple2<String, String>> build() {
        Catalog sourceCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig));

        List<Tuple2<String, String>> result = new ArrayList<>();

        if (database == null) {
            for (String db : sourceCatalog.listDatabases()) {
                try {
                    for (String tbl : sourceCatalog.listTables(db)) {
                        String s = db + "." + tbl;
                        result.add(new Tuple2<>(s, s));
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
        } else if (tableName == null) {
            try {
                for (String tbl : sourceCatalog.listTables(database)) {
                    result.add(new Tuple2<>(database + "." + tbl, targetDatabase + "." + tbl));
                }
            } catch (Exception e) {
                // ignore
            }
        } else {
            result.add(
                    new Tuple2<>(
                            database + "." + tableName, targetDatabase + "." + targetTableName));
        }

        return env.fromCollection(result).forceNonParallel().rebalance();
    }
}
