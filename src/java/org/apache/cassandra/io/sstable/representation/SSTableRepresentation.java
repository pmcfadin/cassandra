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
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An additional physical representation of an sstable's contents, written from the same ordered stream of partitions
 * as the sstable's data and published atomically with it.
 * <p>
 * Every sstable writer for a table with a configured representation (flush, compaction, streaming, scrub, upgrade)
 * feeds a {@link Writer} the exact sequence of partitions, rows and range tombstone markers it writes to its data
 * component. The writer stores its output as one or more components of that sstable. Components are owned by the
 * sstable and therefore follow its lifecycle without further cooperation: they are covered by the lifecycle
 * transaction (removed if the write is aborted or the node crashes before commit), listed in the TOC, hard-linked into
 * snapshots, streamed with entire-sstable streaming when their type is streamable, and deleted when the sstable is
 * obsoleted.
 * <p>
 * The sstable data component remains authoritative: reads, repair and compaction input use it. A representation is
 * derived data that can always be rebuilt from the sstable. Whether a failing representation fails the sstable write
 * or is dropped from it is configured per table, not decided by the implementation.
 * <p>
 * A representation is selected per table with the {@code representation} table option, for example
 * {@code WITH representation = {'class': 'org.example.MyRepresentation', 'required': 'false', 'my_option': 'x'}}.
 * Implementations must have a public constructor taking a single {@code Map<String, String>} of the table's options
 * other than {@code class} and {@code required}, and should reject unknown or malformed options by throwing
 * {@link ConfigurationException} from it.
 */
public interface SSTableRepresentation
{
    /**
     * Validate this representation's options against the table they are set on. Called on the coordinator when a
     * table is created or altered with this representation; not called when nodes load schema.
     */
    default void validate(TableMetadata metadata) throws ConfigurationException
    {
    }

    /**
     * Create a writer for the given new sstable, or return null if this representation does not apply to this table
     * or operation.
     * <p>
     * The writer must not create any file before this method returns, and must only create files named by
     * {@link Descriptor#fileFor(Component)} for components of the given descriptor.
     * <p>
     * Scrub and upgrade operations can rewind the write stream if they encounter corruption, and may not be able to
     * track rewrites correctly. Implementations that cannot handle rewrites should return null for
     * {@code OperationType.SCRUB} and {@code OperationType.UPGRADE_SSTABLES}.
     *
     * @param operation the operation writing the sstable, or null if unknown
     */
    @Nullable
    Writer newWriter(Descriptor descriptor, TableMetadata metadata, SerializationHeader header, @Nullable org.apache.cassandra.db.compaction.OperationType operation);

    /**
     * Receives the content of one sstable in write order. Calls are made from the thread writing the sstable.
     */
    interface Writer
    {
        void startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion);

        void staticRow(Row staticRow);

        /** A row or range tombstone marker of the current partition, in clustering order. */
        void unfiltered(Unfiltered unfiltered);

        void endPartition();

        /**
         * Complete the output and make it durable. Called once, after the last partition and before the sstable's
         * table of contents is written.
         *
         * @return the components written, all of which must exist on disk
         */
        Set<Component> finish() throws IOException;

        /**
         * Release resources and delete any partial output. May be called at any point, including after
         * {@link #finish()} if the enclosing sstable write is aborted.
         */
        void abort(@Nullable Throwable cause);
    }
}
