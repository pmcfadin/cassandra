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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Writes one text line per partition: {@code <hex key> <partition deleted> <rows> <range tombstone markers>}.
 * <p>
 * Option {@code column}: only count rows that have a value for this regular column.
 * Option {@code skip}: comma-separated list of OperationType names for which to return null (skip writing).
 */
public class TestRepresentation implements SSTableRepresentation
{
    public static final Component COMPONENT = Component.Type.createSingleton("TEST_REPRESENTATION", "TestRepresentation.txt", true, null).getSingleton();

    public static final String COLUMN = "column";
    public static final String SKIP = "skip";

    public enum FailAt { CREATE, PARTITION, FINISH }

    public static volatile FailAt failAt;
    public static volatile OperationType lastOperationType;

    private final String column;
    private final EnumSet<OperationType> skipOperations;

    public TestRepresentation(Map<String, String> options)
    {
        Map<String, String> remaining = new HashMap<>(options);
        column = remaining.remove(COLUMN);
        String skipList = remaining.remove(SKIP);
        if (skipList == null)
        {
            skipOperations = EnumSet.noneOf(OperationType.class);
        }
        else
        {
            try
            {
                skipOperations = Arrays.stream(skipList.split(","))
                                       .map(String::trim)
                                       .filter(s -> !s.isEmpty())
                                       .map(OperationType::valueOf)
                                       .collect(Collectors.toCollection(() -> EnumSet.noneOf(OperationType.class)));
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException("Invalid operation type in skip list: " + skipList, e);
            }
        }
        if (!remaining.isEmpty())
            throw new ConfigurationException("Unknown options for TestRepresentation: " + remaining.keySet());
    }

    @Override
    public void validate(TableMetadata metadata)
    {
        if (column != null)
            regularColumn(metadata);
    }

    private ColumnMetadata regularColumn(TableMetadata metadata)
    {
        ColumnMetadata c = metadata.getColumn(ByteBufferUtil.bytes(column));
        if (c == null || !c.isRegular() || c.isComplex())
            throw new ConfigurationException("TestRepresentation column '" + column + "' must be a simple regular column of " + metadata);
        return c;
    }

    @Override
    public Writer newWriter(Descriptor descriptor, TableMetadata metadata, SerializationHeader header, @Nullable OperationType operation)
    {
        lastOperationType = operation;
        if (operation != null && skipOperations.contains(operation))
            return null;
        if (failAt == FailAt.CREATE)
            throw new RuntimeException("injected failure creating representation writer");
        return new TestWriter(descriptor.fileFor(COMPONENT), column == null ? null : regularColumn(metadata));
    }

    private static class TestWriter implements Writer
    {
        private final File file;
        private final ColumnMetadata column;
        private PrintWriter out;
        private DecoratedKey key;
        private boolean deleted;
        private int rows;
        private int markers;

        TestWriter(File file, ColumnMetadata column)
        {
            this.file = file;
            this.column = column;
        }

        @Override
        public void startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion)
        {
            if (failAt == FailAt.PARTITION)
                throw new RuntimeException("injected failure writing representation");

            if (out == null)
            {
                try
                {
                    out = new PrintWriter(new FileOutputStreamPlus(file));
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            this.key = key;
            this.deleted = !partitionLevelDeletion.isLive();
            this.rows = 0;
            this.markers = 0;
        }

        @Override
        public void staticRow(Row staticRow)
        {
        }

        @Override
        public void unfiltered(Unfiltered unfiltered)
        {
            if (!unfiltered.isRow())
                markers++;
            else if (column == null || ((Row) unfiltered).getCell(column) != null)
                rows++;
        }

        @Override
        public void endPartition()
        {
            out.println(ByteBufferUtil.bytesToHex(key.getKey()) + ' ' + deleted + ' ' + rows + ' ' + markers);
        }

        @Override
        public Set<Component> finish() throws IOException
        {
            if (failAt == FailAt.FINISH)
                throw new IOException("injected failure finishing representation");
            if (out == null)
                return Collections.emptySet();
            out.close();
            out = null;
            return Collections.singleton(COMPONENT);
        }

        @Override
        public void abort(Throwable cause)
        {
            if (out != null)
                out.close();
            file.deleteIfExists();
        }
    }
}
