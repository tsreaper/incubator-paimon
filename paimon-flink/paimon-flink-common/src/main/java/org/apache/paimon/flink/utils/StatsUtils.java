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

package org.apache.paimon.flink.utils;

import org.apache.paimon.format.FieldStats;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.Date;

import java.util.HashMap;
import java.util.Map;

/**
 * Utils related to Flink table statistics. Mainly used by Flink planner to choose the optimal
 * execution plan.
 */
public class StatsUtils {

    public static CatalogColumnStatistics toFlinkStats(
            RowType rowType, Iterable<DataFileMeta> metas) {
        Map<String, Long> minLongs = new HashMap<>();
        Map<String, Long> maxLongs = new HashMap<>();
        Map<String, Double> minDoubles = new HashMap<>();
        Map<String, Double> maxDoubles = new HashMap<>();
        Map<String, Date> minDates = new HashMap<>();
        Map<String, Date> maxDates = new HashMap<>();
        Map<String, Long> nullCounts = new HashMap<>();

        FieldStatsArraySerializer serializer = new FieldStatsArraySerializer(rowType);
        for (DataFileMeta meta : metas) {
            FieldStats[] stats = meta.valueStats().fields(serializer);
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String name = rowType.getFieldNames().get(i);
                switch (rowType.getFieldTypes().get(i).getTypeRoot()) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        if (stats[i].minValue() != null) {
                            long min = ((Number) stats[i].minValue()).longValue();
                            minLongs.compute(name, (k, v) -> v == null ? min : Math.min(v, min));
                        }
                        if (stats[i].maxValue() != null) {
                            long max = ((Number) stats[i].maxValue()).longValue();
                            maxLongs.compute(name, (k, v) -> v == null ? max : Math.max(v, max));
                        }
                        break;
                    case FLOAT:
                    case DOUBLE:
                        if (stats[i].minValue() != null) {
                            double min = ((Number) stats[i].minValue()).doubleValue();
                            minDoubles.compute(name, (k, v) -> v == null ? min : Math.min(v, min));
                        }
                        if (stats[i].maxValue() != null) {
                            double max = ((Number) stats[i].maxValue()).doubleValue();
                            maxDoubles.compute(name, (k, v) -> v == null ? max : Math.max(v, max));
                        }
                        break;
                    case DATE:
                        if (stats[i].minValue() != null) {
                            int min = (int) stats[i].minValue();
                            minDates.compute(
                                    name,
                                    (k, v) ->
                                            v == null
                                                    ? new Date(min)
                                                    : new Date(
                                                            Math.min(v.getDaysSinceEpoch(), min)));
                        }
                        if (stats[i].maxValue() != null) {
                            int max = (int) stats[i].maxValue();
                            maxDates.compute(
                                    name,
                                    (k, v) ->
                                            v == null
                                                    ? new Date(max)
                                                    : new Date(
                                                            Math.max(v.getDaysSinceEpoch(), max)));
                        }
                        break;
                }
                if (stats[i].nullCount() != null) {
                    long nullCount = stats[i].nullCount();
                    nullCounts.compute(name, (k, v) -> v == null ? nullCount : v + nullCount);
                }
            }
        }

        Map<String, CatalogColumnStatisticsDataBase> flinkStatsData = new HashMap<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String name = rowType.getFieldNames().get(i);
            switch (rowType.getFieldTypes().get(i).getTypeRoot()) {
                case BOOLEAN:
                    flinkStatsData.put(
                            name,
                            new CatalogColumnStatisticsDataBoolean(
                                    null, null, nullCounts.get(name)));
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    flinkStatsData.put(
                            name,
                            new CatalogColumnStatisticsDataLong(
                                    minLongs.get(name),
                                    maxLongs.get(name),
                                    null,
                                    nullCounts.get(name)));
                    break;
                case FLOAT:
                case DOUBLE:
                    flinkStatsData.put(
                            name,
                            new CatalogColumnStatisticsDataDouble(
                                    minDoubles.get(name),
                                    maxDoubles.get(name),
                                    null,
                                    nullCounts.get(name)));
                    break;
                case DATE:
                    flinkStatsData.put(
                            name,
                            new CatalogColumnStatisticsDataDate(
                                    minDates.get(name),
                                    maxDates.get(name),
                                    null,
                                    nullCounts.get(name)));
                    break;
                case CHAR:
                case VARCHAR:
                    flinkStatsData.put(
                            name,
                            new CatalogColumnStatisticsDataString(
                                    null, null, null, nullCounts.get(name)));
                    break;
                case BINARY:
                case VARBINARY:
                    flinkStatsData.put(
                            name,
                            new CatalogColumnStatisticsDataBinary(
                                    null, null, nullCounts.get(name)));
                    break;
            }
        }
        return new CatalogColumnStatistics(flinkStatsData);
    }
}
