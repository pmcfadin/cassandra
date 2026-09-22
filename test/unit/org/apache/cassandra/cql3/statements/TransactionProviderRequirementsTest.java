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

package org.apache.cassandra.cql3.statements;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.CONDITIONAL;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.MULTI_TABLE;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.READ;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.STRICT_SERIALIZABLE;
import static org.apache.cassandra.service.consensus.txn.TransactionProvider.Capability.WRITE;

public class TransactionProviderRequirementsTest
{
    private static final String KEYSPACE = "txn_provider_requirements";
    private static final TableId TABLE1_ID = TableId.fromString("00000000-0000-0000-0000-000000000201");
    private static final TableId TABLE2_ID = TableId.fromString("00000000-0000-0000-0000-000000000202");
    private static final TableId TABLE3_ID = TableId.fromString("00000000-0000-0000-0000-000000000203");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    parse("CREATE TABLE table1 (k int PRIMARY KEY, v int) " +
                                         "WITH transactional_mode = 'full'", KEYSPACE).id(TABLE1_ID),
                                    parse("CREATE TABLE table2 (k int PRIMARY KEY, v int) " +
                                         "WITH transactional_mode = 'full'", KEYSPACE).id(TABLE2_ID),
                                    parse("CREATE TABLE table3 (k int PRIMARY KEY, v int) " +
                                         "WITH transactional_mode = 'full'", KEYSPACE).id(TABLE3_ID));
    }

    @Test
    public void shouldIncludeLetAndReturnedSelectTables()
    {
        TransactionStatement statement = prepare("BEGIN TRANSACTION\n" +
                                                 "  LET row1 = (SELECT * FROM " + KEYSPACE + ".table1 WHERE k=1);\n" +
                                                 "  SELECT * FROM " + KEYSPACE + ".table2 WHERE k=2;\n" +
                                                 "COMMIT TRANSACTION;");

        Assertions.assertThat(statement.referencedTableIds()).containsExactlyInAnyOrder(TABLE1_ID, TABLE2_ID);
        Assertions.assertThat(statement.requiredCapabilities())
                  .containsExactlyInAnyOrder(STRICT_SERIALIZABLE, READ, MULTI_TABLE);
    }

    @Test
    public void shouldIncludeReturnedReferencesAndUpdateTables()
    {
        TransactionStatement statement = prepare("BEGIN TRANSACTION\n" +
                                                 "  LET row1 = (SELECT * FROM " + KEYSPACE + ".table1 WHERE k=1);\n" +
                                                 "  SELECT row1.v;\n" +
                                                 "  UPDATE " + KEYSPACE + ".table2 SET v=3 WHERE k=2;\n" +
                                                 "COMMIT TRANSACTION;");

        Assertions.assertThat(statement.referencedTableIds()).containsExactlyInAnyOrder(TABLE1_ID, TABLE2_ID);
        Assertions.assertThat(statement.requiredCapabilities())
                  .containsExactlyInAnyOrder(STRICT_SERIALIZABLE, READ, WRITE, MULTI_TABLE);
    }

    @Test
    public void shouldClassifyConditionalTransaction()
    {
        TransactionStatement statement = prepare("BEGIN TRANSACTION\n" +
                                                 "  LET row1 = (SELECT * FROM " + KEYSPACE + ".table1 WHERE k=1);\n" +
                                                 "  IF row1 IS NOT NULL THEN\n" +
                                                 "    UPDATE " + KEYSPACE + ".table1 SET v=3 WHERE k=1;\n" +
                                                 "  END IF\n" +
                                                 "COMMIT TRANSACTION;");

        Assertions.assertThat(statement.referencedTableIds()).containsExactly(TABLE1_ID);
        Assertions.assertThat(statement.requiredCapabilities())
                  .containsExactlyInAnyOrder(STRICT_SERIALIZABLE, READ, WRITE, CONDITIONAL);
    }

    @Test
    public void shouldClassifyReadOnlyReturnedSelect()
    {
        TransactionStatement statement = prepare("BEGIN TRANSACTION\n" +
                                                 "  SELECT * FROM " + KEYSPACE + ".table1 WHERE k=1;\n" +
                                                 "COMMIT TRANSACTION;");

        Assertions.assertThat(statement.referencedTableIds()).containsExactly(TABLE1_ID);
        Assertions.assertThat(statement.requiredCapabilities())
                  .containsExactlyInAnyOrder(STRICT_SERIALIZABLE, READ);
    }

    @Test
    public void shouldClassifyUpdateOnlyTransaction()
    {
        TransactionStatement statement = prepare("BEGIN TRANSACTION\n" +
                                                 "  INSERT INTO " + KEYSPACE + ".table3 (k, v) VALUES (3, 4);\n" +
                                                 "COMMIT TRANSACTION;");

        Assertions.assertThat(statement.referencedTableIds()).containsExactly(TABLE3_ID);
        Assertions.assertThat(statement.requiredCapabilities())
                  .containsExactlyInAnyOrder(STRICT_SERIALIZABLE, READ, WRITE);
    }

    private static TransactionStatement prepare(String query)
    {
        CQLStatement statement = ((TransactionStatement.Parsed) QueryProcessor.parseStatement(query))
                                .prepare(ClientState.forInternalCalls());
        return (TransactionStatement) statement;
    }
}
