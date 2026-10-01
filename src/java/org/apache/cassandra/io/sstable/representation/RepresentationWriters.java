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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

/**
 * The representation writers of one sstable being written. Applies the configured failure policy so that
 * {@link SSTableRepresentation.Writer} implementations do not have to: a failure of a required representation is
 * rethrown and fails the sstable write; a failure of an optional one is logged, its partial output deleted, and the
 * sstable is published without it.
 */
public final class RepresentationWriters
{
    private static final Logger logger = LoggerFactory.getLogger(RepresentationWriters.class);

    public static final RepresentationWriters NONE = new RepresentationWriters(null, Collections.emptyList());

    static final class Entry
    {
        final String name;
        final boolean required;
        final SSTableRepresentation.Writer writer;
        boolean dropped;

        Entry(String name, boolean required, SSTableRepresentation.Writer writer)
        {
            this.name = name;
            this.required = required;
            this.writer = writer;
        }
    }

    private final Descriptor descriptor;
    private final List<Entry> entries;

    RepresentationWriters(Descriptor descriptor, List<Entry> entries)
    {
        this.descriptor = descriptor;
        this.entries = entries;
    }

    public boolean isEmpty()
    {
        return entries.isEmpty();
    }

    public void startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion)
    {
        forEach(w -> w.startPartition(key, partitionLevelDeletion));
    }

    public void staticRow(Row staticRow)
    {
        forEach(w -> w.staticRow(staticRow));
    }

    public void unfiltered(Unfiltered unfiltered)
    {
        forEach(w -> w.unfiltered(unfiltered));
    }

    public void endPartition()
    {
        forEach(SSTableRepresentation.Writer::endPartition);
    }

    /**
     * Finish every representation.
     *
     * @return the components to add to the sstable
     */
    public Set<Component> finish()
    {
        if (entries.isEmpty())
            return Collections.emptySet();

        Set<Component> components = new HashSet<>();
        for (Entry entry : entries)
        {
            if (entry.dropped)
                continue;

            try
            {
                Set<Component> written = entry.writer.finish();
                for (Component component : written)
                {
                    if (!descriptor.fileFor(component).exists())
                        throw new IOException("Representation " + entry.name + " reported component " + component + " of " + descriptor + " that does not exist");
                }
                components.addAll(written);
            }
            catch (Throwable t)
            {
                onFailure(entry, t);
            }
        }
        return components;
    }

    public Throwable abort(@Nullable Throwable accumulate)
    {
        for (Entry entry : entries)
        {
            if (entry.dropped)
                continue;

            entry.dropped = true;
            accumulate = Throwables.perform(accumulate, () -> entry.writer.abort(null));
        }
        return accumulate;
    }

    private void forEach(Consumer<SSTableRepresentation.Writer> action)
    {
        for (int i = 0; i < entries.size(); i++)
        {
            Entry entry = entries.get(i);
            if (entry.dropped)
                continue;

            try
            {
                action.accept(entry.writer);
            }
            catch (Throwable t)
            {
                onFailure(entry, t);
            }
        }
    }

    private void onFailure(Entry entry, Throwable t)
    {
        JVMStabilityInspector.inspectThrowable(t);
        if (entry.required)
            throw Throwables.unchecked(t);

        logger.warn("Failed to write sstable representation {} for {}; publishing the sstable without it", entry.name, descriptor, t);
        entry.dropped = true;
        try
        {
            entry.writer.abort(t);
        }
        catch (Throwable t2)
        {
            JVMStabilityInspector.inspectThrowable(t2);
            logger.warn("Failed to abort sstable representation {} for {}", entry.name, descriptor, t2);
        }
    }
}
