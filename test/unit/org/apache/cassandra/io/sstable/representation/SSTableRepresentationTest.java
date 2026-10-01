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

package org.apache.cassandra.io.sstable.representation;

import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.RepresentationParams;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SSTableRepresentationTest extends CQLTester
{
    private static final String WITH_REPRESENTATION = " WITH representation = {'class': 'TestRepresentation'}";
    private static final String WITH_REQUIRED_REPRESENTATION = " WITH representation = {'class': 'TestRepresentation', 'required': 'true'}";

    @After
    public void resetFailures()
    {
        TestRepresentation.failAt = null;
        TestRepresentation.lastOperationType = null;
    }

    private static List<String> representationLines(SSTableReader sstable) throws Exception
    {
        return Files.readAllLines(sstable.descriptor.fileFor(TestRepresentation.COMPONENT).toPath());
    }

    private static String line(int pk, boolean deleted, int rows, int markers)
    {
        return ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(pk)) + ' ' + deleted + ' ' + rows + ' ' + markers;
    }

    private static boolean representationFileExists(Descriptor descriptor)
    {
        return descriptor.fileFor(TestRepresentation.COMPONENT).exists();
    }

    private static boolean hasRepresentation(SSTableReader sstable)
    {
        return sstable.getComponents().contains(TestRepresentation.COMPONENT) && representationFileExists(sstable.descriptor);
    }

    private static List<String> representationFiles(File directory)
    {
        return List.of(directory.tryListNames()).stream()
                   .filter(name -> name.endsWith(TestRepresentation.COMPONENT.name))
                   .collect(Collectors.toList());
    }

    @Test
    public void testFlushPublishesRepresentationWithSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))" + WITH_REPRESENTATION);
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 1)");
        execute("DELETE FROM %s WHERE pk = 3");
        execute("DELETE FROM %s WHERE pk = 4 AND ck > 1 AND ck < 5");
        flush();

        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        assertThat(sstable.getComponents()).contains(TestRepresentation.COMPONENT);
        assertThat(TOCComponent.loadTOC(sstable.descriptor)).contains(TestRepresentation.COMPONENT);
        assertThat(representationLines(sstable)).containsExactlyInAnyOrder(line(1, false, 2, 0),
                                                                           line(2, false, 1, 0),
                                                                           line(3, true, 0, 0),
                                                                           line(4, false, 0, 2));
    }

    @Test
    public void testOnlySelectedTablesHaveRepresentation() throws Throwable
    {
        String with = createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REPRESENTATION);
        String without = createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (1, 1)", KEYSPACE, with));
        execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (1, 1)", KEYSPACE, without));
        flush(KEYSPACE, with);
        flush(KEYSPACE, without);

        assertThat(getColumnFamilyStore(KEYSPACE, with).getLiveSSTables()).hasSize(1).allMatch(SSTableRepresentationTest::hasRepresentation);
        assertThat(getColumnFamilyStore(KEYSPACE, without).getLiveSSTables()).hasSize(1).noneMatch(s -> representationFileExists(s.descriptor));
    }

    @Test
    public void testAlterTableEnablesAndDisablesRepresentation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (pk, v) VALUES (1, 1)");
        flush();
        alterTable("ALTER TABLE %s" + WITH_REPRESENTATION);
        execute("INSERT INTO %s (pk, v) VALUES (2, 1)");
        flush();
        alterTable("ALTER TABLE %s WITH representation = {}");
        execute("INSERT INTO %s (pk, v) VALUES (3, 1)");
        flush();

        List<Boolean> withRepresentation = cfs.getLiveSSTables().stream()
                                              .sorted((a, b) -> Long.compare(a.getMaxTimestamp(), b.getMaxTimestamp()))
                                              .map(SSTableRepresentationTest::hasRepresentation)
                                              .collect(Collectors.toList());
        assertThat(withRepresentation).containsExactly(false, true, false);
        assertThat(cfs.metadata().params.representation).isEqualTo(RepresentationParams.NONE);
    }

    @Test
    public void testTableOptionsAreValidatedAndPassedToTheRepresentation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) " +
                    "WITH representation = {'class': 'TestRepresentation', 'column': 'w'}");
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        flush();

        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        assertThat(representationLines(sstable)).containsExactly(line(1, false, 1, 0));

        assertThat(getCurrentColumnFamilyStore().metadata().toCqlString(false, false, false))
            .contains("AND representation = {'class': 'TestRepresentation', 'column': 'w', 'required': 'false'}");

        assertInvalidMessage("must be a simple regular column",
                             "ALTER TABLE %s WITH representation = {'class': 'TestRepresentation', 'column': 'ck'}");
        assertInvalidMessage("Unknown options for TestRepresentation: [colum]",
                             "ALTER TABLE %s WITH representation = {'class': 'TestRepresentation', 'colum': 'w'}");
        assertInvalidMessage("Unable to find sstable representation class",
                             "ALTER TABLE %s WITH representation = {'class': 'org.example.NoSuchRepresentation'}");
        assertInvalidMessage("Missing sub-option 'class'",
                             "ALTER TABLE %s WITH representation = {'column': 'w'}");
        assertInvalidMessage("must be true or false",
                             "ALTER TABLE %s WITH representation = {'class': 'TestRepresentation', 'required': 'yes'}");

        // rejected alterations leave the table unchanged
        assertThat(getCurrentColumnFamilyStore().metadata().params.representation.options).isEqualTo(Map.of("column", "w"));
    }

    @Test
    public void testRepresentationIsOnlySerializedFromV11() throws Throwable
    {
        TableParams params = TableParams.builder()
                                        .representation(RepresentationParams.fromMap(Map.of("class", "TestRepresentation", "column", "v")))
                                        .build();

        assertThat(roundTrip(params, Version.V11)).isEqualTo(params);
        assertThat(roundTrip(params, Version.V10).representation).isEqualTo(RepresentationParams.NONE);
    }

    private static TableParams roundTrip(TableParams params, Version version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TableParams.serializer.serialize(params, out, version);
            assertThat((long) out.getLength()).isEqualTo(TableParams.serializer.serializedSize(params, version));
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                return TableParams.serializer.deserialize(in, version);
            }
        }
    }

    @Test
    public void testMissingImplementationOnlyFailsRequiredRepresentation()
    {
        assertThat(RepresentationParams.fromMap(Map.of("class", "org.example.NoSuchRepresentation")).representation()).isNull();
        assertThatThrownBy(() -> RepresentationParams.fromMap(Map.of("class", "org.example.NoSuchRepresentation", "required", "true")).representation())
            .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void testCompactionRewritesRepresentationAndDeletesInputs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))" + WITH_REPRESENTATION);
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 1)");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        execute("DELETE FROM %s WHERE pk = 2");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<Descriptor> inputs = cfs.getLiveSSTables().stream().map(s -> s.descriptor).collect(Collectors.toSet());
        assertThat(inputs).hasSize(2).allMatch(SSTableRepresentationTest::representationFileExists);

        compact();
        LifecycleTransaction.waitForDeletions();

        SSTableReader output = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertThat(representationLines(output)).containsExactlyInAnyOrder(line(1, false, 2, 0),
                                                                           line(2, true, 0, 0));
        assertThat(inputs).noneMatch(SSTableRepresentationTest::representationFileExists);
        assertThat(representationFiles(output.descriptor.directory)).hasSize(1);
    }

    @Test
    public void testOptionalRepresentationFailureStillPublishesSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REPRESENTATION);

        for (TestRepresentation.FailAt failAt : TestRepresentation.FailAt.values())
        {
            TestRepresentation.failAt = failAt;
            execute("INSERT INTO %s (pk, v) VALUES (?, 1)", failAt.ordinal());
            flush();
        }

        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertThat(sstables).hasSize(TestRepresentation.FailAt.values().length);
        for (SSTableReader sstable : sstables)
        {
            assertThat(sstable.getComponents()).doesNotContain(TestRepresentation.COMPONENT);
            assertThat(TOCComponent.loadTOC(sstable.descriptor)).doesNotContain(TestRepresentation.COMPONENT);
            assertThat(representationFileExists(sstable.descriptor)).isFalse();
        }
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s"), row(0), row(1), row(2));
    }

    @Test
    public void testRequiredRepresentationFailureAbortsSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REQUIRED_REPRESENTATION);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File directory = cfs.getDirectories().getDirectoryForNewSSTables();

        TestRepresentation.failAt = TestRepresentation.FailAt.FINISH;
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        SSTableWriter writer = newWriter(cfs, directory, txn);
        writer.append(UpdateBuilder.create(cfs.metadata(), 1).newRow().add("v", 1).build().unfilteredIterator());
        assertThatThrownBy(writer::prepareToCommit).hasMessageContaining("injected failure");
        writer.abort();
        txn.abort();
        LifecycleTransaction.waitForDeletions();

        assertThat(directory.tryListNames()).noneMatch(name -> name.startsWith(writer.descriptor.baseFile().name()));
        assertThat(cfs.getLiveSSTables()).isEmpty();
    }

    @Test
    public void testRequiredRepresentationCreationFailureFailsWriterCreation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REQUIRED_REPRESENTATION);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File directory = cfs.getDirectories().getDirectoryForNewSSTables();

        TestRepresentation.failAt = TestRepresentation.FailAt.CREATE;
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        assertThatThrownBy(() -> newWriter(cfs, directory, txn)).hasMessageContaining("injected failure");
        txn.abort();
        LifecycleTransaction.waitForDeletions();

        assertThat(representationFiles(directory)).isEmpty();
    }

    @Test
    public void testAbortedWriteRemovesRepresentation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REPRESENTATION);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File directory = cfs.getDirectories().getDirectoryForNewSSTables();

        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        SSTableWriter writer = newWriter(cfs, directory, txn);
        writer.append(UpdateBuilder.create(cfs.metadata(), 1).newRow().add("v", 1).build().unfilteredIterator());
        assertThat(representationFileExists(writer.descriptor)).isTrue();

        writer.abort();
        txn.abort();
        LifecycleTransaction.waitForDeletions();

        assertThat(representationFileExists(writer.descriptor)).isFalse();
        assertThat(directory.tryListNames()).noneMatch(name -> name.startsWith(writer.descriptor.baseFile().name()));
    }

    @Test
    public void testUncommittedWriteIsRemovedOnRestart() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REPRESENTATION);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File directory = cfs.getDirectories().getDirectoryForNewSSTables();

        // write and prepare, but crash before the transaction commits
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        SSTableWriter writer = newWriter(cfs, directory, txn);
        writer.append(UpdateBuilder.create(cfs.metadata(), 1).newRow().add("v", 1).build().unfilteredIterator());
        writer.prepareToCommit();
        assertThat(TOCComponent.loadTOC(writer.descriptor)).contains(TestRepresentation.COMPONENT);
        assertThat(representationFileExists(writer.descriptor)).isTrue();

        // startup cleanup of the unfinished transaction log
        assertThat(LifecycleTransaction.removeUnfinishedLeftovers(cfs)).isTrue();
        assertThat(representationFileExists(writer.descriptor)).isFalse();
        assertThat(directory.tryListNames()).noneMatch(name -> name.startsWith(writer.descriptor.baseFile().name()));

        // release the in-memory state of the "crashed" write
        try
        {
            writer.abort();
            txn.abort();
        }
        catch (Throwable ignored)
        {
        }
    }

    @Test
    public void testSnapshotAndReopenIncludeRepresentation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REPRESENTATION);
        execute("INSERT INTO %s (pk, v) VALUES (1, 1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        SnapshotManager.instance.takeSnapshot("representation", cfs.getKeyspaceTableName());
        File snapshotDirectory = cfs.getDirectories().getSnapshotDirectory(sstable.descriptor, "representation");
        assertThat(representationFiles(snapshotDirectory)).hasSize(1);

        SSTableReader reopened = SSTableReader.open(cfs, sstable.descriptor);
        try
        {
            assertThat(reopened.getComponents()).contains(TestRepresentation.COMPONENT);
            assertThat(reopened.getStreamingComponents()).contains(TestRepresentation.COMPONENT);
        }
        finally
        {
            reopened.selfRef().release();
        }
    }

    @Test
    public void testFlushPassesFlushOperationType() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)" + WITH_REPRESENTATION);
        execute("INSERT INTO %s (pk, v) VALUES (1, 1)");
        flush();

        assertThat(TestRepresentation.lastOperationType).isEqualTo(OperationType.FLUSH);
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1).allMatch(SSTableRepresentationTest::hasRepresentation);
    }

    @Test
    public void testCompactionPassesCompactionOperationType() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))" + WITH_REPRESENTATION);
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        flush();

        TestRepresentation.lastOperationType = null;
        compact();

        assertThat(TestRepresentation.lastOperationType).isNotNull().isNotEqualTo(OperationType.FLUSH);
        assertThat(TestRepresentation.lastOperationType.writesData).isTrue();
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1).allMatch(SSTableRepresentationTest::hasRepresentation);
    }

    @Test
    public void testSkipOperationTypeOmitsRepresentation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH representation = {'class': 'TestRepresentation', 'skip': 'SCRUB'}");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (pk, v) VALUES (1, 1)");
        flush();
        assertThat(cfs.getLiveSSTables()).hasSize(1).allMatch(SSTableRepresentationTest::hasRepresentation);

        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
             IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), new IScrubber.Options.Builder().checkData().build()))
        {
            scrubber.scrub();
        }
        LifecycleTransaction.waitForDeletions();

        assertThat(cfs.getLiveSSTables()).hasSize(1).noneMatch(SSTableRepresentationTest::hasRepresentation);
    }

    @Test
    public void testSkipOperationTypeDoesNotAffectOtherOperations() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH representation = {'class': 'TestRepresentation', 'skip': 'SCRUB,UPGRADE_SSTABLES'}");

        execute("INSERT INTO %s (pk, v) VALUES (1, 1)");
        flush();
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1).allMatch(SSTableRepresentationTest::hasRepresentation);

        execute("INSERT INTO %s (pk, v) VALUES (2, 2)");
        flush();
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(2).allMatch(SSTableRepresentationTest::hasRepresentation);
    }

    private static SSTableWriter newWriter(ColumnFamilyStore cfs, File directory, LifecycleTransaction txn)
    {
        Descriptor desc = cfs.newSSTableDescriptor(directory);
        return desc.getFormat().getWriterFactory().builder(desc)
                   .setTableMetadataRef(cfs.metadata)
                   .setKeyCount(0)
                   .setSerializationHeader(new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS))
                   .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                   .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                   .addDefaultComponents(cfs.indexManager.listIndexGroups())
                   .build(txn, cfs);
    }
}
