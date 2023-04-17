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

package org.apache.paimon.tests;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.util.UUID;

/** Tests for {@code FlinkActions}. */
public class FlinkActionsE2eTest extends E2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkActionsE2eTest.class);

    public FlinkActionsE2eTest() {
        super(true, false);
    }

    private String warehousePath;
    private String catalogDdl;
    private String useCatalogCmd;

    @BeforeEach
    public void setUp() {
        warehousePath = TEST_DATA_DIR + "/" + UUID.randomUUID() + ".store";
        catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        warehousePath);

        useCatalogCmd = "USE CATALOG ts_catalog;";
    }

    @RepeatedTest(10)
    public void testDelete() throws Exception {
        String tableDdl =
                "CREATE TABLE IF NOT EXISTS ts_table (\n"
                        + "    dt STRING,\n"
                        + "    k int,\n"
                        + "    v int,\n"
                        + "    PRIMARY KEY (k, dt) NOT ENFORCED\n"
                        + ") PARTITIONED BY (dt);";

        String insert =
                "INSERT INTO ts_table VALUES ('2023-01-13', 0, 15), ('2023-01-14', 0, 19), ('2023-01-13', 0, 39), "
                        + "('2023-01-15', 0, 34), ('2023-01-15', 0, 56), ('2023-01-15', 0, 37), "
                        + "('2023-01-16', 1, 25), ('2023-01-17', 1, 50), ('2023-01-18', 1, 75), "
                        + "('2023-01-19', 1, 23), ('2023-01-20', 1, 28), ('2023-01-21', 1, 31);";

        runSql("SET 'table.dml-sync' = 'true';\n" + insert, catalogDdl, useCatalogCmd, tableDdl);

        // run delete job
        Container.ExecResult execResult =
                jobManager.execInContainer(
                        "bin/flink",
                        "run",
                        "-p",
                        "1",
                        "-c",
                        "org.apache.paimon.flink.action.FlinkActions",
                        "-Dclassloader.resolve-order=parent-first",
                        "lib/paimon-flink.jar",
                        "delete",
                        "--warehouse",
                        warehousePath,
                        "--database",
                        "default",
                        "--table",
                        "ts_table",
                        "--where",
                        "dt < '2023-01-17'");

        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());

        // read all data from paimon
        runSql(
                "INSERT INTO result1 SELECT * FROM ts_table;",
                catalogDdl,
                useCatalogCmd,
                tableDdl,
                createResultSink("result1", "dt STRING, k INT, v INT"));

        // check the left data
        checkResult(
                "2023-01-17, 1, 50",
                "2023-01-18, 1, 75",
                "2023-01-19, 1, 23",
                "2023-01-20, 1, 28",
                "2023-01-21, 1, 31");
    }

    private void runSql(String sql, String... ddls) throws Exception {
        runSql(String.join("\n", ddls) + "\n" + sql);
    }
}
