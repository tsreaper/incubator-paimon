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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

/** Factory to create {@link HiveCatalog}. */
public class HiveCatalogFactory implements CatalogFactory {

    private static final String IDENTIFIER = "hive";

    private static final ConfigOption<String> METASTORE_CLIENT_CLASS =
            ConfigOptions.key("metastore.client.class")
                    .stringType()
                    .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
                    .withDescription(
                            "Class name of Hive metastore client.\n"
                                    + "NOTE: This class must directly implements "
                                    + "org.apache.hadoop.hive.metastore.IMetaStoreClient.");

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        String uri =
                Preconditions.checkNotNull(
                        context.options().get(CatalogOptions.URI),
                        CatalogOptions.URI.key()
                                + " must be set for paimon "
                                + IDENTIFIER
                                + " catalog");

        Configuration hadoopConfig = new Configuration();
        context.options().toMap().forEach(hadoopConfig::set);
        hadoopConfig.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        hadoopConfig.set(
                HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse.toUri().toString());

        return new HiveCatalog(fileIO, hadoopConfig, context.options().get(METASTORE_CLIENT_CLASS));
    }
}
