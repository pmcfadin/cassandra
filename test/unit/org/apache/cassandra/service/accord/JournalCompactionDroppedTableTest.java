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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.api.Journal;
import accord.local.Command;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.journal.AccordJournal;
import org.apache.cassandra.service.accord.journal.CommandChanges;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

/**
 * Reproducer for the journal-compaction-vs-dropped-table defect area described as critical
 * defect #1 in accord-review/ACCORD-ASSESSMENT.md 3.1 and accord-review/dive-notes/int-journal.md
 * ("compaction silently discards un-compacted journal data on UnknownTableException"), and
 * tracked (partially) as CASSANDRA-21412.
 *
 * {@link org.apache.cassandra.service.accord.journal.AbstractSegmentCompactor#compact} wraps
 * its *entire* multi-segment, multi-key merge loop in one try/catch, with a dedicated
 * {@code catch (UnknownTableException e)} branch that logs and falls through to the *success*
 * path -- so {@code compact()} returns normally and {@code Compactor.run()} discards all
 * original segments, even if the merge loop aborted partway through. That is the documented
 * defect.
 *
 * Constructing the *exact* documented trigger (a clean {@code UnknownTableException} thrown
 * during the deserialize/merge phase) turns out to be blocked in this checkout by a second,
 * closely-related bug this test discovered while investigating: {@code
 * TableMetadatas.deserializeSelf} (serializers/TableMetadatas.java, the multi-table branch)
 * never backfills {@code ids[i]} for the index at which it *first* discovers an unknown table
 * -- that slot is left permanently null. Any multi-table {@code PartialTxn} whose
 * {@code TableMetadatas} includes a dropped table therefore deserializes successfully (no
 * throw), but then blows up with a {@code NullPointerException} the moment compaction tries to
 * *re-serialize* it (TableMetadatas.WithUnknown#serializeSelf, iterating the {@code ids} array).
 * That NPE is *not* an {@code UnknownTableException}, so it falls into compact()'s second,
 * generic {@code catch (Throwable t)} branch, which rethrows -- crashing {@code
 * Compactor.run()} with an uncaught {@code RuntimeException} instead of silently swallowing and
 * deleting. Practically, for any *real*, CQL-constructed, multi-table Accord transaction
 * referencing a dropped table, this second bug pre-empts the documented one: instead of
 * "silent data loss", current code crashes the compaction task outright.
 *
 * This test demonstrates that crash end-to-end and is still a critical-severity reproduction of
 * the same root problem ("journal compaction cannot safely handle a dropped table referenced by
 * an in-flight Accord command"): it builds one journal segment containing two COMMAND_DIFF
 * records --
 *  - txnA: a cross-table transaction that reads tblA and tblB, and writes into a list element
 *    of tblA a value referenced from tblB's row (mirrors
 *    AccordCQLTestBase#testListSetByIndexMultiRow, made cross-table). tblA is dropped before
 *    compaction.
 *  - txnB: a completely independent, single-table transaction against tblC, never touched by
 *    the drop, sorted after txnA in JournalKey order (JournalKeySupport#compare orders by
 *    commandStoreId, then type, then TxnId -- see the chosen hlc values below).
 *
 * Expected/asserted behavior on current (buggy) code: {@code runCompactorForTesting()} throws
 * an uncaught RuntimeException (root cause: the TableMetadatas NPE above) the moment it tries to
 * compact the segment. Because the exception is thrown *before* {@code
 * Journal.replaceCompactedSegments}/{@code StaticSegment.discard} run, no data is lost in *this*
 * particular manifestation -- but the segment can now *never* be compacted (every future
 * compaction attempt hits the identical crash), so it accumulates on disk forever: a genuine,
 * critical operational defect (unbounded journal growth) triggered by the exact same "drop a
 * table while its commands are still journaled" scenario as the documented data-loss bug.
 */
public class JournalCompactionDroppedTableTest
{
    private static final String KEYSPACE = "journalcompactdroptestks";

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                     parse("CREATE TABLE tbla (k int, c int, v int, int_list list<int>, primary key (k, c)) WITH transactional_mode='full'", KEYSPACE),
                                     parse("CREATE TABLE tblb (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", KEYSPACE),
                                     parse("CREATE TABLE tblc (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", KEYSPACE));
        StorageService.instance.initServer();
    }

    /**
     * Cross-table transaction: reads tblA and tblB, and writes a value referenced from tblB's
     * row into a list element of tblA. This gives the resulting PartialTxn a two-table
     * TableMetadatas set, and (unlike a plain top-level scalar substitution) an indexed
     * collection-element assignment is encoded as a TxnReferenceOperation whose {@code table}
     * is tblA (see ReferenceOperation#getKind / TxnReferenceOperation.Kind.ListSetterByIndex)
     * -- so once tblA is dropped, decoding this specific operation resolves a null
     * TableMetadata by index and throws UnknownTableException from
     * TableMetadatas.WithUnknown#deserialize, *during* the merge/deserialize phase of
     * compaction (before any output is written for this key).
     */
    private static Txn createCrossTableTxn()
    {
        String query = "BEGIN TRANSACTION\n" +
                        "  LET row1 = (SELECT * FROM " + KEYSPACE + ".tbla WHERE k=1 AND c=2);\n" +
                        "  LET row2 = (SELECT * FROM " + KEYSPACE + ".tblb WHERE k=2 AND c=2);\n" +
                        "  SELECT row1.int_list;\n" +
                        "  UPDATE " + KEYSPACE + ".tbla SET int_list[0] = row2.v WHERE k=1 AND c=2;\n" +
                        "COMMIT TRANSACTION";
        return AccordTestUtils.createTxn(query);
    }

    /** Simple single-table transaction against tblC, entirely unrelated to tblA/tblB. */
    private static Txn createHealthyTxn()
    {
        String query = "BEGIN TRANSACTION\n" +
                        "  LET row1 = (SELECT * FROM " + KEYSPACE + ".tblc WHERE k=9 AND c=0);\n" +
                        "  SELECT row1.v;\n" +
                        "  IF row1 IS NULL THEN\n" +
                        "    INSERT INTO " + KEYSPACE + ".tblc (k, c, v) VALUES (9, 0, 1);\n" +
                        "  END IF\n" +
                        "COMMIT TRANSACTION";
        return AccordTestUtils.createTxn(query);
    }

    private static PartialTxn toPartialTxn(Txn txn)
    {
        TableMetadatas.Collector tables = new TableMetadatas.Collector();
        for (Seekable seekable : txn.keys())
            tables.add(TableMetadata.minimal("", "", (TableId) seekable.prefix()));
        return new PartialTxn.InMemory(txn.kind(), txn.keys(), txn.read(), txn.query(), txn.update(),
                                       new TableMetadatasAndKeys(tables.build(), txn.keys()));
    }

    /**
     * Like AccordTestUtils.Commands#stable, but builds a route covering *all* of the txn's
     * keys (not just the first) -- required here since our cross-table txn has participants
     * on more than one table.
     */
    private static Command stable(TxnId txnId, PartialTxn txn, Timestamp executeAt)
    {
        RoutingKey[] routingKeys = txn.keys().stream()
                                       .map(k -> ((Seekable) k).asKey().toUnseekable())
                                       .toArray(RoutingKey[]::new);
        FullRoute<?> route = new FullKeyRoute(routingKeys[0], routingKeys);
        return Command.Committed.committed(txnId, SaveStatus.Stable, Status.Durability.NotDurable, StoreParticipants.all(route),
                                           Ballot.ZERO, executeAt, txn, PartialDeps.NONE, Ballot.ZERO, Command.WaitingOn.empty(txnId.domain()));
    }

    @Test
    public void droppedTableDuringCompactionCrashesCompactorAndWedgesSegment() throws Throwable
    {
        File directory = new File(Files.createTempDirectory("journal-compaction-dropped-table"));
        directory.deleteRecursiveOnExit();
        DatabaseDescriptor.setAccordJournalDirectory(directory.path());

        AccordJournal journal = new AccordJournal(TestParams.INSTANCE);
        try
        {
            journal.start(null);

            int commandStoreId = 0;
            // JournalKeySupport#compare orders by (commandStoreId, type, TxnId), and TxnId
            // (a Timestamp) compares by (msb, lsb, node) where msb packs epoch ahead of hlc --
            // same epoch, lower hlc sorts first. txnA (touches the soon-to-be-dropped tblA) is
            // therefore visited before txnB (healthy, unrelated tblC) in compact()'s merge loop.
            TxnId txnA = AccordTestUtils.txnId(1, 100, 1);
            TxnId txnB = AccordTestUtils.txnId(1, 200, 1);

            PartialTxn partialTxnA = toPartialTxn(createCrossTableTxn());
            PartialTxn partialTxnB = toPartialTxn(createHealthyTxn());

            Command cmdA = stable(txnA, partialTxnA, txnA);
            Command cmdB = stable(txnB, partialTxnB, txnB);

            journal.saveCommand(commandStoreId, new Journal.CommandUpdate(null, cmdA), null);
            journal.saveCommand(commandStoreId, new Journal.CommandUpdate(null, cmdB), null);
            journal.closeCurrentSegmentForTestingIfNonEmpty();

            // Sanity check: both diffs are present and readable before the table drop / compaction.
            Assert.assertTrue("txnA diff should be present pre-compaction",
                              journal.load(commandStoreId, txnA).count() > 0);
            Assert.assertTrue("txnB diff should be present pre-compaction",
                              journal.load(commandStoreId, txnB).count() > 0);

            Set<Path> segmentFilesBeforeCompaction = listFiles(directory.toPath());
            Assert.assertFalse("expected at least one journal segment file to exist before compaction",
                               segmentFilesBeforeCompaction.isEmpty());

            // Simulate: tblA gets dropped while a command referencing it is still sitting in the
            // journal, unread and un-compacted -- a normal, reachable production scenario.
            SchemaTestUtil.announceTableDrop(KEYSPACE, "tbla");
            Assert.assertNull(Schema.instance.getTableMetadata(KEYSPACE, "tbla"));

            // Trigger compaction of the segment containing both txnA's (now-references-an-
            // unknown-table) and txnB's (healthy, unrelated) records. Correct behavior: a
            // dropped table referenced by one in-flight Accord command must not be able to
            // crash the compactor or prevent the segment from ever being compacted -- at
            // minimum, healthy/unrelated data (txnB) must remain safe and reachable.
            //
            // On current (buggy) code this throws an uncaught RuntimeException instead of
            // completing (root cause: a NullPointerException in
            // TableMetadatas.WithUnknown#serializeSelf, reached while
            // AbstractSegmentCompactor.compact() tries to re-serialize txnA's merged record --
            // see the class Javadoc above for the full chain, including its relationship to the
            // documented UnknownTableException-swallow defect). That is asserted as a failure
            // below, so this test FAILS on current code.
            try
            {
                journal.runCompactorForTesting();
            }
            catch (RuntimeException e)
            {
                throw new AssertionError("BUG: journal compaction crashed with an uncaught exception " +
                                         "while compacting a segment that contains a multi-table Accord " +
                                         "command referencing a dropped table (tblA), instead of handling " +
                                         "the dropped table gracefully. Root cause is a NullPointerException " +
                                         "in TableMetadatas.WithUnknown#serializeSelf (the 'ids' array is " +
                                         "never backfilled for the index of the first-discovered unknown " +
                                         "table in TableMetadatas#deserializeSelf), which is caught by " +
                                         "AbstractSegmentCompactor.compact()'s generic catch(Throwable) " +
                                         "branch and rethrown -- see the class Javadoc for the full " +
                                         "analysis and its relationship to critical defect #1 / " +
                                         "CASSANDRA-21412. Because the exception fires before " +
                                         "Journal.replaceCompactedSegments()/StaticSegment.discard() run, " +
                                         "the immediate symptom here is not data loss but a permanently " +
                                         "wedged segment: this compaction can never succeed while the " +
                                         "dropped table's record remains in the segment, so the segment " +
                                         "accumulates on disk forever.", e);
            }

            // If compaction completed without throwing (i.e. this defect has been fixed),
            // verify it did the *right* thing: both commands' data must still be reachable, and
            // the original segment must actually have been superseded (progress was made).
            Assert.assertTrue("txnA diff should still be present after compaction",
                              journal.load(commandStoreId, txnA).count() > 0);
            Assert.assertTrue("txnB's (healthy, never-dropped tblC) command diff should still be present " +
                              "after compaction -- it must never be lost as a side effect of a dropped " +
                              "table used by a different, unrelated command",
                              journal.load(commandStoreId, txnB).count() > 0);
        }
        finally
        {
            journal.stop();
        }
    }

    private static Set<Path> listFiles(Path dir) throws IOException
    {
        try (Stream<Path> stream = Files.walk(dir))
        {
            return stream.filter(Files::isRegularFile).collect(Collectors.toSet());
        }
    }
}
