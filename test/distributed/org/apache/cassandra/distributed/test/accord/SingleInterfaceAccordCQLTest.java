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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;

import org.junit.BeforeClass;

import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/** Runs the existing full-mode CQL regressions on hosts without extra loopback aliases. */
public class SingleInterfaceAccordCQLTest extends AccordCQLTestBase
{
    public SingleInterfaceAccordCQLTest()
    {
        super(TransactionalMode.full);
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder.withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                                                     .appendConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                                   .set("paxos_variant", PaxosVariant.v2.name())
                                                                                   .set("accord.migration_concurrency", "17")), 2);
        SHARED_CLUSTER.schemaChange("CREATE TYPE " + KEYSPACE + ".person (height int, age int)");
    }
}
