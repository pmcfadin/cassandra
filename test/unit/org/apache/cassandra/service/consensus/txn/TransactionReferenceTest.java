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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.SimplePartition;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TransactionReferenceTest
{
    private static final ByteBuffer KEY = Int32Type.instance.decompose(7);
    private static final ByteBuffer CLUSTERING = Int32Type.instance.decompose(9);
    private static final ByteBuffer VALUE = Int32Type.instance.decompose(11);

    @Test
    public void readsKeysRegularStaticAndMissingValues()
    {
        TableMetadata table = TableMetadata.builder("ks", "reference")
                                            .addPartitionKeyColumn("pk", Int32Type.instance)
                                            .addClusteringColumn("ck", Int32Type.instance)
                                            .addStaticColumn("s", Int32Type.instance)
                                            .addRegularColumn("v", Int32Type.instance)
                                            .partitioner(Murmur3Partitioner.instance)
                                            .build();
        ColumnMetadata pk = column(table, "pk");
        ColumnMetadata ck = column(table, "ck");
        ColumnMetadata statik = column(table, "s");
        ColumnMetadata value = column(table, "v");

        SimplePartition source = new SimplePartition(table, table.partitioner.decorateKey(KEY));
        source.add(Clustering.STATIC_CLUSTERING).add(statik, VALUE).build();
        source.add(BufferClustering.make(CLUSTERING)).add(value, VALUE).build();
        FilteredPartition partition = materialize(source);

        assertThat(new TransactionReference(0, table, pk, null).toByteBuffer(partition, Int32Type.instance)).isEqualTo(KEY);
        assertThat(new TransactionReference(0, table, ck, null).toByteBuffer(partition, Int32Type.instance)).isEqualTo(CLUSTERING);
        assertThat(new TransactionReference(0, table, statik, null).toByteBuffer(partition, Int32Type.instance)).isEqualTo(VALUE);
        assertThat(new TransactionReference(0, table, value, null).toByteBuffer(partition, Int32Type.instance)).isEqualTo(VALUE);
        assertThat(new TransactionReference(0, table, column(table, "v"), null).row(partition)).isNotNull();

        TableMetadata missingTable = TableMetadata.builder("ks", "missing")
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("v", Int32Type.instance)
                                                   .partitioner(Murmur3Partitioner.instance)
                                                   .build();
        FilteredPartition empty = materialize(new SimplePartition(missingTable, missingTable.partitioner.decorateKey(KEY)));
        assertThat(new TransactionReference(0, missingTable, column(missingTable, "v"), null).toByteBuffer(empty, Int32Type.instance)).isNull();
    }

    @Test
    public void readsCollectionAndUdtPaths()
    {
        FieldIdentifier field = FieldIdentifier.forInternalString("field");
        UserType udt = new UserType("ks", ByteBufferUtil.bytes("reference_udt"), Collections.singletonList(field),
                                    Collections.singletonList(Int32Type.instance), true);
        ListType<Integer> list = ListType.getInstance(Int32Type.instance, true);
        SetType<Integer> set = SetType.getInstance(Int32Type.instance, true);
        SetType<Integer> frozenSet = set.freeze();
        UserType frozenUdt = udt.freeze();
        TableMetadata table = TableMetadata.builder("ks", "collections")
                                            .addPartitionKeyColumn("pk", Int32Type.instance)
                                            .addClusteringColumn("ck", Int32Type.instance)
                                            .addRegularColumn("list", list)
                                            .addRegularColumn("set", set)
                                            .addRegularColumn("frozen_set", frozenSet)
                                            .addRegularColumn("udt", udt)
                                            .addRegularColumn("frozen_udt", frozenUdt)
                                            .partitioner(Murmur3Partitioner.instance)
                                            .build();
        ColumnMetadata listColumn = column(table, "list");
        ColumnMetadata setColumn = column(table, "set");
        ColumnMetadata frozenSetColumn = column(table, "frozen_set");
        ColumnMetadata udtColumn = column(table, "udt");
        ColumnMetadata frozenUdtColumn = column(table, "frozen_udt");
        ByteBuffer second = Int32Type.instance.decompose(12);
        SimplePartition source = new SimplePartition(table, table.partitioner.decorateKey(KEY));
        source.add(BufferClustering.make(CLUSTERING))
              .addComplex(listColumn, Arrays.asList(VALUE, second))
              .addComplex(setColumn, Collections.singletonList(VALUE))
              .add(frozenSetColumn, frozenSet.pack(Collections.singletonList(VALUE)))
              .addComplex(udtColumn, Collections.singletonList(VALUE))
              .add(frozenUdtColumn, frozenUdt.pack(Collections.singletonList(VALUE)))
              .build();
        FilteredPartition partition = materialize(source);

        assertThat(new TransactionReference(0, table, listColumn, CellPath.create(Int32Type.instance.decompose(1)))
                   .toByteBuffer(partition, Int32Type.instance)).isEqualTo(second);
        assertThat(new TransactionReference(0, table, setColumn, CellPath.create(VALUE))
                   .toByteBuffer(partition, Int32Type.instance)).isEqualTo(VALUE);
        assertThat(new TransactionReference(0, table, frozenSetColumn, CellPath.create(VALUE))
                   .toByteBuffer(partition, Int32Type.instance)).isEqualTo(VALUE);
        CellPath fieldPath = udt.cellPathForField(field);
        assertThat(new TransactionReference(0, table, udtColumn, fieldPath)
                   .toByteBuffer(partition, Int32Type.instance)).isEqualTo(VALUE);
        assertThat(new TransactionReference(0, table, frozenUdtColumn, fieldPath)
                   .toByteBuffer(partition, Int32Type.instance)).isEqualTo(VALUE);
    }

    @Test
    public void copiesCellPathBuffers()
    {
        TableMetadata table = TableMetadata.builder("ks", "path")
                                            .addPartitionKeyColumn("pk", Int32Type.instance)
                                            .addRegularColumn("v", ListType.getInstance(Int32Type.instance, true))
                                            .partitioner(Murmur3Partitioner.instance)
                                            .build();
        ColumnMetadata value = column(table, "v");
        ByteBuffer input = Int32Type.instance.decompose(1);
        TransactionReference reference = new TransactionReference(0, table, value, CellPath.create(input));

        input.putInt(input.position(), 2);
        assertThat(Int32Type.instance.compose(reference.path().get(0))).isEqualTo(1);

        ByteBuffer exposed = reference.path().get(0);
        exposed.putInt(exposed.position(), 3);
        assertThat(Int32Type.instance.compose(reference.path().get(0))).isEqualTo(1);
    }

    private static ColumnMetadata column(TableMetadata table, String name)
    {
        return table.getExistingColumn(ColumnIdentifier.getInterned(name, true));
    }

    private static FilteredPartition materialize(SimplePartition source)
    {
        try (RowIterator rows = source.filtered())
        {
            return FilteredPartition.create(rows);
        }
    }

    @Test
    public void validatesReceiverEvenWhenPartitionIsMissing()
    {
        TableMetadata table = TableMetadata.builder("ks", "missing_receiver")
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addRegularColumn("v", Int32Type.instance)
                                           .partitioner(Murmur3Partitioner.instance)
                                           .build();
        TransactionReference reference = new TransactionReference(0, table, column(table, "v"), null);
        assertThatThrownBy(() -> reference.toByteBuffer(null, UTF8Type.instance))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void outcomeIsImmutableAndDistinguishesNoOp()
    {
        Map<Integer, FilteredPartition> partitions = new HashMap<>();
        TransactionOutcome outcome = new TransactionOutcome(123, partitions);
        partitions.put(1, null);

        assertThat(outcome.atMicros()).isEqualTo(123);
        assertThat(outcome.partitions()).isEmpty();
        assertThat(outcome.isNoOp()).isFalse();
        assertThat(TransactionOutcome.noOp().isNoOp()).isTrue();
        assertThat(TransactionOutcome.noOp().partitions()).isEmpty();
    }
}
