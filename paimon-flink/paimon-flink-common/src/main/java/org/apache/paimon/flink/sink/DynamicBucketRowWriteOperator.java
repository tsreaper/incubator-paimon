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

import org.apache.paimon.Magic;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link PrepareCommitOperator} to write {@link InternalRow} with bucket. Record schema is fixed.
 */
public class DynamicBucketRowWriteOperator
        extends TableWriteOperator<Tuple2<InternalRow, Integer>> {

    private static final long serialVersionUID = 1L;

    public DynamicBucketRowWriteOperator(
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(table, storeSinkWriteProvider, initialCommitUser);
    }

    @Override
    protected boolean containLogSystem() {
        return false;
    }

    @Override
    public void processElement(StreamRecord<Tuple2<InternalRow, Integer>> element)
            throws Exception {
        if (Magic.M.get()) {
            if (element.getValue().f0.getFieldCount() == 1) {
                System.out.println(
                        System.currentTimeMillis() + " ~~~~~~~ " + element.getValue().f0.getInt(0));
            } else if (element.getValue().f0.getFieldCount() == 4) {
                System.out.println(
                        System.currentTimeMillis()
                                + " ~~~~~~~ "
                                + element.getValue().f0.getInt(0)
                                + " !!!!!!!!!! "
                                + element.getValue().f0.getInt(1));
            }
        }
        write.write(element.getValue().f0, element.getValue().f1);
    }
}
