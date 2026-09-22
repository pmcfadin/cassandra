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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TransactionPlanTest
{
    private static final ColumnIdentifier PARTITION_KEY = ColumnIdentifier.getInterned("pk", false);
    private static final ColumnIdentifier VALUE = ColumnIdentifier.getInterned("value", false);
    private static final TableMetadata TABLE = TableMetadata.builder("ks", "tbl")
                                                            .addPartitionKeyColumn(PARTITION_KEY, Int32Type.instance)
                                                            .addRegularColumn(VALUE, Int32Type.instance)
                                                            .partitioner(Murmur3Partitioner.instance)
                                                            .build();
    private static final ColumnMetadata VALUE_COLUMN = TABLE.getColumn(VALUE);

    @Test
    public void readSlotIdsRemainCompatibleWithAccordNames()
    {
        assertThat(TransactionReadSlot.id(TransactionReadSlot.Kind.USER)).isEqualTo(0);
        assertThat(TransactionReadSlot.id(TransactionReadSlot.Kind.RETURNING)).isEqualTo(1 << 26);
        assertThat(TransactionReadSlot.id(TransactionReadSlot.Kind.AUTO_READ, 7)).isEqualTo((2 << 26) | 7);
        assertThat(TransactionReadSlot.kind((2 << 26) | 7)).isEqualTo(TransactionReadSlot.Kind.AUTO_READ);
        assertThat(TransactionReadSlot.index((2 << 26) | 7)).isEqualTo(7);

        assertThatThrownBy(() -> TransactionReadSlot.id(TransactionReadSlot.Kind.USER, 1 << 26))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void literalCopiesInputAndPreservesNullEmptyAndUnset()
    {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{ 1, 2 });
        TransactionValue value = TransactionValue.literal(source);
        source.put(0, (byte) 9);

        assertThat(value.literal()).isEqualTo(ByteBuffer.wrap(new byte[]{ 1, 2 }));
        assertThat(TransactionValue.literal(null).literal()).isNull();
        assertThat(TransactionValue.literal(ByteBufferUtil.EMPTY_BYTE_BUFFER).literal())
                .isEqualTo(ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .isNotSameAs(ByteBufferUtil.UNSET_BYTE_BUFFER);
        assertThat(TransactionValue.literal(ByteBufferUtil.UNSET_BYTE_BUFFER).literal()).isSameAs(ByteBufferUtil.UNSET_BYTE_BUFFER);
        assertThat(TransactionValue.literal(ByteBufferUtil.EMPTY_BYTE_BUFFER))
                .isNotEqualTo(TransactionValue.literal(ByteBufferUtil.UNSET_BYTE_BUFFER));

        ByteBuffer returned = value.literal();
        returned.position(1);
        assertThat(value.literal().position()).isZero();
        returned.put(0, (byte) 7);
        assertThat(value.literal()).isEqualTo(ByteBuffer.wrap(new byte[]{ 1, 2 }));
    }

    @Test
    public void conditionAndOperationBuffersAreDefensiveCopies()
    {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{ 3, 4 });
        TransactionReference reference = new TransactionReference(0, TABLE, VALUE_COLUMN, null);
        TransactionCondition condition = new TransactionCondition(TransactionCondition.Kind.EQUAL, reference, source);
        ByteBuffer conditionValue = condition.value();
        conditionValue.position(1);
        assertThat(condition.value().position()).isZero();
        conditionValue.put(0, (byte) 8);
        assertThat(condition.value()).isEqualTo(source);

        TransactionOperation operation = new TransactionOperation(TransactionOperation.Kind.ConstantSetter,
                                                                  VALUE_COLUMN,
                                                                  TABLE,
                                                                  source,
                                                                  source,
                                                                  TransactionValue.literal(source));
        ByteBuffer key = operation.keyOrIndex();
        key.position(1);
        assertThat(operation.keyOrIndex().position()).isZero();
        key.put(0, (byte) 9);
        assertThat(operation.keyOrIndex()).isEqualTo(source);
        operation.field().put(0, (byte) 9);
        assertThat(operation.field()).isEqualTo(source);

        assertThat(new TransactionCondition(TransactionCondition.Kind.EQUAL, reference, ByteBufferUtil.EMPTY_BYTE_BUFFER))
                .isNotEqualTo(new TransactionCondition(TransactionCondition.Kind.EQUAL, reference, ByteBufferUtil.UNSET_BYTE_BUFFER));
    }

    @Test
    public void boundValuesWorkWithCassandraTypeComparators()
    {
        ByteBuffer expected = ByteBufferUtil.bytes(42);
        TransactionReference reference = new TransactionReference(0, TABLE, VALUE_COLUMN, null);
        TransactionValue value = TransactionValue.literal(expected);
        TransactionCondition condition = new TransactionCondition(TransactionCondition.Kind.EQUAL, reference, expected);

        // Cassandra's unsafe comparator treats a buffer without an accessible array as native memory.
        assertThat(value.literal().hasArray()).isTrue();
        assertThat(condition.value().hasArray()).isTrue();
        assertThat(Int32Type.instance.compare(value.literal(), expected)).isZero();
        assertThat(Int32Type.instance.compare(condition.value(), expected)).isZero();
    }

    @Test
    public void planAndReturnSelectionDefensivelyCopyCollections()
    {
        List<Integer> slots = new ArrayList<>(Collections.singletonList(1));
        TransactionPlan.ReturnSelection returning = TransactionPlan.ReturnSelection.rows(slots);
        slots.add(2);

        assertThat(returning.slots()).containsExactly(1);
        assertThatThrownBy(() -> returning.slots().add(2)).isInstanceOf(UnsupportedOperationException.class);

        List<TransactionCondition> conditions = new ArrayList<>();
        TransactionPlan plan = new TransactionPlan(Collections.emptyList(),
                                                   Collections.emptyList(),
                                                   conditions,
                                                   Collections.emptyMap(),
                                                   returning,
                                                   42,
                                                   false);
        conditions.add(null);

        assertThat(plan.conditions()).isEmpty();
        assertThat(plan.tables()).isEmpty();
        assertThat(plan.minEpoch()).isEqualTo(42);
        assertThat(plan.isNoOp()).isFalse();
    }
}
