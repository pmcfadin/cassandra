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

package org.apache.cassandra.service.consensus.txn;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;

public class EtcdTransactionCompileTest
{
    private static final String KEYSPACE = "etcd_compile";
    private static final String TABLE = "values";
    private static final TableId TABLE_ID = TableId.fromString("00000000-0000-0000-0000-000000000801");
    private static final UUID DOMAIN_ID = UUID.fromString("00000000-0000-0000-0000-000000000802");
    private static final ExternalTransactionDomainBinding BINDING =
            new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000803", "7");

    private ClusterMetadataService previousMetadataService;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE " + TABLE + " (pk int PRIMARY KEY, v int) WITH transactional_mode = 'full'", KEYSPACE)
                                     .id(TABLE_ID).build();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), table);
    }

    @Before
    public void installActiveMetadata()
    {
        previousMetadataService = ClusterMetadataService.instance();
        TableMetadata table = TransactionDomainGuard.markReserved(table(), DOMAIN_ID);
        TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(DOMAIN_ID, TABLE_ID,
                                                                                   EtcdScalarRequest.PROVIDER,
                                                                                   EtcdScalarRequest.PROFILE,
                                                                                   1, 1, 1, 1,
                                                                                   Collections.singleton(new NodeId(1)))
                                                    .activate(BINDING);
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(table));
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY,
                                                       new DistributedSchema(ClusterMetadata.current().schema.getKeyspaces().withAddedOrUpdated(keyspace)))
                                   .transformer()
                                   .with(new ConsistencyDomains(Epoch.FIRST, Collections.singletonMap(TABLE_ID, descriptor)))
                                   .build().metadata;
        metadata = metadata.forceEpoch(Epoch.EMPTY);
        metadata.schema.initializeKeyspaceInstances(ClusterMetadata.current().schema, false);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(metadata));
    }

    @After
    public void restoreMetadata()
    {
        ClusterMetadataService.unsetInstance();
        if (previousMetadataService != null)
            ClusterMetadataService.setInstance(previousMetadataService);
    }

    @Test
    public void compilesConstantWriteWithoutContactingEtcd()
    {
        EtcdScalarRequest request = compile("BEGIN TRANSACTION\n" +
                                            "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1;\n" +
                                            "COMMIT TRANSACTION;");

        Assertions.assertThat(request.writes).isNotEmpty();
        Assertions.assertThat(request.writes).anyMatch(write -> Integer.valueOf(7).equals(write.constant));
    }

    @Test
    public void commonCompilerProducesImmutableNeutralShape()
    {
        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1;\n" +
                                    "COMMIT TRANSACTION;");
        ScalarTransactionPlan scalar = scalarCompile(plan);

        Assertions.assertThat(scalar.reads).isEmpty();
        Assertions.assertThat(scalar.conditions).isEmpty();
        Assertions.assertThat(scalar.writes).singleElement()
                  .satisfies(write -> {
                      Assertions.assertThat(write.key).isEqualTo(1);
                      Assertions.assertThat(write.constant).isEqualTo(7);
                      Assertions.assertThat(write.sourceSlot).isNull();
                  });
        Assertions.assertThatThrownBy(() -> scalar.writes.clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void commonCompilerMaterializesAbsentRows()
    {
        TransactionPlan plan = plan("BEGIN TRANSACTION\n" +
                                    "  SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1;\n" +
                                    "COMMIT TRANSACTION;");
        HashMap<Integer, Integer> reads = new HashMap<>();
        int slot = plan.reads().get(0).slot();
        reads.put(slot, null);
        TransactionOutcome outcome = ScalarTransactionPlan.materialize(plan, 1234, reads);

        Assertions.assertThat(outcome.atMicros()).isEqualTo(1234);
        Assertions.assertThat(outcome.partition(slot)).isNotNull();
        Assertions.assertThat(outcome.partition(slot).isEmpty()).isTrue();
    }

    @Test
    public void compilesReferenceCopyAndConditions()
    {
        EtcdScalarRequest request = compile("BEGIN TRANSACTION\n" +
                                            "  LET source = (SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1);\n" +
                                            "  IF source.v = 4 AND source IS NOT NULL THEN\n" +
                                            "    UPDATE " + KEYSPACE + '.' + TABLE + " SET v = source.v WHERE pk = 2;\n" +
                                            "  END IF\n" +
                                            "COMMIT TRANSACTION;");

        Assertions.assertThat(request.conditions).hasSize(2);
        Assertions.assertThat(request.conditions).anyMatch(condition -> condition.operator == EtcdScalarRequest.Operator.EQ && condition.value == 4);
        Assertions.assertThat(request.conditions).anyMatch(condition -> condition.rowReference && condition.operator == EtcdScalarRequest.Operator.IS_NOT_NULL);
        Assertions.assertThat(request.writes).anyMatch(write -> write.constant == null && write.sourceSlot >= 0);
    }

    @Test
    public void projectionChangesStableRequestShapeAndHash()
    {
        EtcdScalarRequest valueOnly = compile("BEGIN TRANSACTION\n" +
                                              "  SELECT v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1;\n" +
                                              "COMMIT TRANSACTION;");
        EtcdScalarRequest keyAndValue = compile("BEGIN TRANSACTION\n" +
                                                "  SELECT pk, v FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1;\n" +
                                                "COMMIT TRANSACTION;");

        Assertions.assertThat(valueOnly.returnShape).isNotEqualTo(keyAndValue.returnShape);
        Assertions.assertThat(valueOnly.contentHash()).isNotEqualTo(keyAndValue.contentHash());
    }

    @Test
    public void rejectsUnsupportedShapeBeforeEtcd()
    {
        assertCompileRejected("BEGIN TRANSACTION\n  SELECT writetime(v) FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1;\nCOMMIT TRANSACTION;");
        assertCompileRejected("BEGIN TRANSACTION\n  SELECT ttl(v) FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1;\nCOMMIT TRANSACTION;");
        assertCompileRejected("BEGIN TRANSACTION\n  DELETE FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1;\nCOMMIT TRANSACTION;");
        assertCompileRejected("BEGIN TRANSACTION\n  UPDATE " + KEYSPACE + '.' + TABLE + " USING TTL 10 SET v = 7 WHERE pk = 1;\nCOMMIT TRANSACTION;");
        assertCompileRejected("BEGIN TRANSACTION\n  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1;\nCOMMIT TRANSACTION;", ConsistencyLevel.QUORUM);
    }

    @Test
    public void rejectsStaleDomainBeforeEtcd()
    {
        TransactionPlan plan = plan("BEGIN TRANSACTION\n  UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1;\nCOMMIT TRANSACTION;");
        TransactionExecutionContext stale = new TransactionExecutionContext(new TransactionDomain("00000000-0000-0000-0000-000000000804", EtcdScalarRequest.PROVIDER),
                                                                              ConsistencyLevel.ONE, ConsistencyLevel.SERIAL,
                                                                              ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
        EtcdTransactionProvider provider = new EtcdTransactionProvider(new EtcdScalarStore(Collections.singletonList("http://127.0.0.1:1"), 25));
        Assertions.assertThatThrownBy(() -> provider.compile(plan, stale, UUID.randomUUID()))
                  .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void rejectsSchemaReplacementBeforeEtcd()
    {
        TransactionPlan preparedPlan = plan("BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + TABLE + " SET v = 7 WHERE pk = 1; COMMIT TRANSACTION;");
        ClusterMetadata current = ClusterMetadata.current();
        TableMetadata replacement = table().unbuild().comment("stale schema").build();
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(replacement));
        ClusterMetadata staleSchema = new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY,
                                                          new DistributedSchema(ClusterMetadata.current().schema.getKeyspaces().withAddedOrUpdated(keyspace)))
                                      .transformer()
                                      .with(new ConsistencyDomains(Epoch.FIRST, Collections.singletonMap(TABLE_ID,
                                                                                                         new TransactionDomainDescriptor(DOMAIN_ID, TABLE_ID,
                                                                                                                                          EtcdScalarRequest.PROVIDER,
                                                                                                                                          EtcdScalarRequest.PROFILE,
                                                                                                                                          1, 1, 1, 1,
                                                                                                                                          Collections.singleton(new NodeId(1)))
                                                                                                         .activate(BINDING))))
                                      .build().metadata;
        staleSchema = staleSchema.forceEpoch(Epoch.EMPTY);
        staleSchema.schema.initializeKeyspaceInstances(current.schema, false);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(staleSchema));
        try
        {
            Assertions.assertThatThrownBy(() -> compile(preparedPlan, ConsistencyLevel.ONE))
                      .isInstanceOf(InvalidRequestException.class);
        }
        finally
        {
            ClusterMetadataService.unsetInstance();
            ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(current));
        }
    }

    private static void assertCompileRejected(String query)
    {
        assertCompileRejected(query, ConsistencyLevel.ONE);
    }

    private static void assertCompileRejected(String query, ConsistencyLevel consistency)
    {
        TransactionExecutionContext context = new TransactionExecutionContext(new TransactionDomain(DOMAIN_ID.toString(), EtcdScalarRequest.PROVIDER),
                                                                                consistency, ConsistencyLevel.SERIAL,
                                                                                ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
        EtcdTransactionProvider provider = new EtcdTransactionProvider(new EtcdScalarStore(Collections.singletonList("http://127.0.0.1:1"), 25));
        Assertions.assertThatThrownBy(() -> provider.compile(plan(query), context, UUID.randomUUID()))
                  .isInstanceOf(InvalidRequestException.class);
    }

    private static EtcdScalarRequest compile(String query)
    {
        return compile(plan(query), ConsistencyLevel.ONE);
    }

    private static EtcdScalarRequest compile(TransactionPlan plan, ConsistencyLevel consistency)
    {
        TransactionExecutionContext context = new TransactionExecutionContext(new TransactionDomain(DOMAIN_ID.toString(), EtcdScalarRequest.PROVIDER),
                                                                                consistency, ConsistencyLevel.SERIAL,
                                                                                ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
        return new EtcdTransactionProvider(new EtcdScalarStore(Collections.singletonList("http://127.0.0.1:1"), 25))
               .compile(plan, context, UUID.fromString("00000000-0000-0000-0000-000000000805"));
    }

    private static ScalarTransactionPlan scalarCompile(TransactionPlan plan)
    {
        TransactionExecutionContext context = new TransactionExecutionContext(new TransactionDomain(DOMAIN_ID.toString(), EtcdScalarRequest.PROVIDER),
                                                                                ConsistencyLevel.ONE, ConsistencyLevel.SERIAL,
                                                                                ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
        return ScalarTransactionPlan.compile(plan, context);
    }

    private static TransactionPlan plan(String query)
    {
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        CQLStatement statement = parsed.prepare(ClientState.forInternalCalls());
        TransactionStatement transaction = (TransactionStatement) statement;
        QueryOptions options = QueryProcessor.makeInternalOptions(transaction, new Object[0]);
        return transaction.toPlan(ClientState.forInternalCalls(), options);
    }

    private static TableMetadata table()
    {
        return org.apache.cassandra.schema.Schema.instance.getTableMetadata(KEYSPACE, TABLE);
    }
}
