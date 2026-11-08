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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Read;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TxnRead.slice / intersecting must keep one read per maximal run of touching intersecting sub-ranges of a range read.
 * A range read is a single TxnNamedRead keyed by a (wide) token range, so several sub-ranges all intersect the same
 * item; sub-ranges that touch merge into one read.
 */
public class TxnReadTest
{
    private static final TableId TABLE = TableId.fromLong(42);
    private static final TableId TABLE2 = TableId.fromLong(43);
    private static TableMetadata table;
    private static TableMetadata table2;
    private static TableMetadatas tables;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        table = TableMetadata.builder("ks", "tbl", TABLE)
                             .addPartitionKeyColumn("key", BytesType.instance)
                             .partitioner(Murmur3Partitioner.instance)
                             .build();
        table2 = TableMetadata.builder("ks", "tbl2", TABLE2)
                              .addPartitionKeyColumn("key", BytesType.instance)
                              .partitioner(Murmur3Partitioner.instance)
                              .build();
        tables = TableMetadatas.of(table);
    }

    private static TokenRange r(long start, long end)
    {
        return r(TABLE, start, end);
    }

    private static TokenRange r(TableId table, long start, long end)
    {
        return TokenRange.create(table, new LongToken(start), new LongToken(end));
    }

    private static TxnRead rangeRead(long start, long end)
    {
        PartitionRangeReadCommand command = PartitionRangeReadCommand.allDataRead(table, 0);
        AbstractBounds<PartitionPosition> bounds = new Bounds<>(new LongToken(start).minKeyBound(), new LongToken(end).maxKeyBound());
        return TxnRead.createRangeRead(tables, command, bounds, ConsistencyLevel.ONE);
    }

    private static Ranges keysOf(Read read)
    {
        return (Ranges) ((TxnRead) read).keys();
    }

    private static void assertRead(Read read, Range... expected)
    {
        TxnRead txnRead = (TxnRead) read;
        assertThat(keysOf(read)).isEqualTo(Ranges.of(expected));
        assertThat(txnRead.keys().size()).isEqualTo(expected.length);
        // the TxnRead constructor already enforces this on every result; this guards against that check being relaxed
        assertThat(keysOf(read).mergeTouching()).isEqualTo(keysOf(read));
        txnRead.validateOrder();
    }

    private static void assertSelects(TxnRead read, Ranges select, Range... expected)
    {
        // independent of select's walk: the item keys sliced to the selection, with touching slices merged
        Ranges oracle = ((Ranges) read.keys()).slice(select, Routables.Slice.Minimal).mergeTouching();
        assertThat(Ranges.of(expected)).describedAs("expected ranges disagree with the slice(Minimal).mergeTouching() oracle").isEqualTo(oracle);
        assertRead(read.slice(select), expected);
        assertRead(read.intersecting(select), expected);
    }

    private static Ranges ranges(Range... ranges)
    {
        return Ranges.of(ranges);
    }

    private static TokenKey tk(long token)
    {
        return new TokenKey(TABLE, new LongToken(token));
    }

    @Test
    public void sliceKeepsEverySubRange()
    {
        TxnRead read = rangeRead(0, 1000);
        // sanity: the item key is the wide range, whatever its exact endpoints
        Range wide = (Range) read.keys().get(0);
        assertThat(wide.compareIntersecting(r(100, 200))).isEqualTo(0);

        assertRead(read.slice(ranges(r(100, 200), r(300, 400))), r(100, 200), r(300, 400));
        assertRead(read.slice(ranges(r(100, 200), r(300, 400), r(500, 600))), r(100, 200), r(300, 400), r(500, 600));
    }

    /**
     * Execution looks each piece up with forEachWithKey (TxnRead.readDirect). Each piece of a read sliced into
     * non-touching runs must find exactly its own item, and a range in a gap must find none.
     */
    @Test
    public void forEachWithKeyFindsEveryNonTouchingItem()
    {
        TxnRead read = rangeRead(0, 2000);
        for (int n = 1; n <= 5; n++)
        {
            Range[] pieces = new Range[n];
            for (int i = 0; i < n; i++)
                pieces[i] = r(100 + 200L * i, 200 + 200L * i);
            TxnRead sliced = (TxnRead) read.slice(ranges(pieces));
            assertRead(sliced, pieces);

            for (int i = 0; i < n; i++)
            {
                Range piece = pieces[i];
                List<Range> found = new ArrayList<>();
                sliced.forEachWithKey(piece, item -> found.add(item.key().asRange()));
                assertThat(found).describedAs("n=%d, key %s", n, piece).containsExactly(piece);

                Range inner = r(110 + 200L * i, 190 + 200L * i);
                List<Range> foundInner = new ArrayList<>();
                sliced.forEachWithKey(inner, item -> foundInner.add(item.key().asRange()));
                assertThat(foundInner).describedAs("n=%d, key %s inside %s", n, inner, piece).containsExactly(piece);
            }
            for (int i = 0; i <= n; i++)
            {
                Range gap = r(10 + 200L * i, 90 + 200L * i);
                sliced.forEachWithKey(gap, item -> { throw new AssertionError("gap " + gap + " matched " + item.key()); });
            }
        }
    }

    @Test
    public void intersectingKeepsEverySubRange()
    {
        TxnRead read = rangeRead(0, 1000);
        assertRead(read.intersecting(ranges(r(100, 200), r(300, 400))), r(100, 200), r(300, 400));
        assertRead(read.intersecting(ranges(r(100, 200), r(300, 400), r(500, 600))), r(100, 200), r(300, 400), r(500, 600));
    }

    @Test
    public void singleSubRange()
    {
        TxnRead read = rangeRead(0, 1000);
        assertRead(read.slice(ranges(r(100, 200))), r(100, 200));
        assertRead(read.intersecting(ranges(r(100, 200))), r(100, 200));
    }

    @Test
    public void noIntersection()
    {
        TxnRead read = rangeRead(0, 1000);
        Ranges outside = ranges(r(2000, 3000), r(4000, 5000));
        Seekables<?, ?> sliced = ((TxnRead) read.slice(outside)).keys();
        assertThat(sliced.isEmpty()).isTrue();
        assertThat(((TxnRead) read.intersecting(outside)).keys().isEmpty()).isTrue();
    }

    @Test
    public void rangesAtItemBoundary()
    {
        // item covers (min(0)-1, 1000] after the bounds conversion; sub-ranges clipped to it
        TxnRead read = rangeRead(0, 1000);
        Range wide = (Range) read.keys().get(0);
        TokenRange before = r(-500, 100);
        TokenRange after = r(900, 5000);
        TxnRead sliced = (TxnRead) read.slice(ranges(before, r(300, 400), after));
        Ranges expected = ranges(before, r(300, 400), after).slice(Ranges.of(wide), Routables.Slice.Minimal);
        assertThat(keysOf(sliced)).isEqualTo(expected);
        assertThat(expected.size()).isEqualTo(3);
        sliced.validateOrder();
    }

    private static TxnRead twoItemRead(long s1, long e1, long s2, long e2)
    {
        PartitionRangeReadCommand command = PartitionRangeReadCommand.allDataRead(table, 0);
        List<TxnNamedRead> items = new ArrayList<>();
        items.add(new TxnNamedRead(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 0), new Bounds<>(new LongToken(s1).minKeyBound(), new LongToken(e1).maxKeyBound()), command, tables));
        items.add(new TxnNamedRead(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 1), new Bounds<>(new LongToken(s2).minKeyBound(), new LongToken(e2).maxKeyBound()), command, tables));
        return TxnRead.createTxnRead(tables, items, ConsistencyLevel.ONE, accord.primitives.Routable.Domain.Range);
    }

    @Test
    public void twoItemsInterleavedRanges()
    {
        // two items (as produced by merging reads of disjoint ranges): (0,1000] and (2000,3000]
        TxnRead merged = twoItemRead(0, 1000, 2000, 3000);
        assertThat(merged.keys().size()).isEqualTo(2);

        Ranges in = ranges(r(100, 200), r(300, 400), r(2100, 2200), r(2500, 2600), r(5000, 6000));
        List<Range> expected = new ArrayList<>();
        expected.add(r(100, 200));
        expected.add(r(300, 400));
        expected.add(r(2100, 2200));
        expected.add(r(2500, 2600));
        assertRead(merged.slice(in), expected.toArray(new Range[0]));
        assertRead(merged.intersecting(in), expected.toArray(new Range[0]));

        // one sub-range spanning the gap between the items touches both
        Ranges spanning = ranges(r(500, 2500));
        Ranges exp = spanning.slice(((Ranges) merged.keys()), Routables.Slice.Minimal);
        assertThat(keysOf(merged.slice(spanning))).isEqualTo(exp);
    }

    @Test
    public void touchingSubRangesMergeIntoOne()
    {
        // (100,200], (200,300] and (300,400] touch, so they must come back as the single range (100,400]
        TxnRead read = rangeRead(0, 1000);
        assertSelects(read, ranges(r(100, 200), r(200, 300), r(300, 400)), r(100, 400));
    }

    @Test
    public void touchingRunsWithGaps()
    {
        TxnRead read = rangeRead(0, 1000);
        assertSelects(read, ranges(r(100, 200), r(200, 300), r(500, 600), r(600, 700), r(800, 900)),
                      r(100, 300), r(500, 700), r(800, 900));
    }

    @Test
    public void twoItemsTouchingRunsStaySeparate()
    {
        TxnRead merged = twoItemRead(0, 1000, 2000, 3000);
        Ranges in = ranges(r(100, 200), r(200, 300), r(2100, 2200), r(2200, 2300));
        for (TxnRead sliced : new TxnRead[] { (TxnRead) merged.slice(in), (TxnRead) merged.intersecting(in) })
        {
            assertRead(sliced, r(100, 300), r(2100, 2300));
            assertThat(sliced.get(0).txnDataName()).isEqualTo(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 0));
            assertThat(sliced.get(1).txnDataName()).isEqualTo(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 1));
        }
    }

    @Test
    public void touchingRunMergesBackIntoWholeItem()
    {
        // (-500,500] and (500,5000] slice to two halves of the item (before(0),1000] that merge back into the item key
        TxnRead read = rangeRead(0, 1000);
        Range item = (Range) read.keys().get(0);
        assertSelects(read, ranges(r(-500, 500), r(500, 5000)), item);
    }

    @Test
    public void touchingRunStartingAtItemEdge()
    {
        // the item starts at TokenKey.before(0), so a run can start exactly on its edge
        TxnRead read = rangeRead(0, 1000);
        TokenKey edge = TokenKey.before(TABLE, new LongToken(0));
        assertSelects(read, ranges(TokenRange.create(edge, tk(200)), r(200, 300)), TokenRange.create(edge, tk(300)));
    }

    @Test
    public void touchingRunsAcrossTwoTables()
    {
        TableMetadatas both = TableMetadatas.of(List.of(table, table2));
        PartitionRangeReadCommand command1 = PartitionRangeReadCommand.allDataRead(table, 0);
        PartitionRangeReadCommand command2 = PartitionRangeReadCommand.allDataRead(table2, 0);
        List<TxnNamedRead> items = new ArrayList<>();
        items.add(new TxnNamedRead(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 0), new Bounds<>(new LongToken(0).minKeyBound(), new LongToken(1000).maxKeyBound()), command1, both));
        items.add(new TxnNamedRead(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 1), new Bounds<>(new LongToken(0).minKeyBound(), new LongToken(1000).maxKeyBound()), command2, both));
        TxnRead read = TxnRead.createTxnRead(both, items, ConsistencyLevel.ONE, accord.primitives.Routable.Domain.Range);

        Ranges in = ranges(r(TABLE, 100, 200), r(TABLE, 200, 300), r(TABLE2, 100, 200), r(TABLE2, 200, 300));
        for (TxnRead sliced : new TxnRead[] { (TxnRead) read.slice(in), (TxnRead) read.intersecting(in) })
        {
            assertRead(sliced, r(TABLE, 100, 300), r(TABLE2, 100, 300));
            assertThat(sliced.get(0).txnDataName()).isEqualTo(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 0));
            assertThat(sliced.get(1).txnDataName()).isEqualTo(TxnData.txnDataName(TxnData.TxnDataNameKind.USER, 1));
        }
    }

    @Test
    public void touchingRunsAcrossMinMaxSentinels()
    {
        // a whole-table read is keyed (min, max]; runs that start at min and end at max merge back into the item key
        PartitionRangeReadCommand command = PartitionRangeReadCommand.allDataRead(table, 0);
        LongToken minimum = Murmur3Partitioner.MINIMUM;
        TxnRead read = TxnRead.createRangeRead(tables, command, new Bounds<>(minimum.minKeyBound(), minimum.maxKeyBound()), ConsistencyLevel.ONE);
        Range item = (Range) read.keys().get(0);
        assertThat(((TokenRange) item).isFullRange()).isTrue();

        TokenKey min = TokenKey.min(TABLE, Murmur3Partitioner.instance);
        TokenKey max = TokenKey.max(TABLE, Murmur3Partitioner.instance);
        assertSelects(read, ranges(TokenRange.create(min, tk(200)), r(200, 300), TokenRange.create(tk(300), max)), item);
        assertSelects(read, ranges(TokenRange.create(min, tk(200)), r(200, 300), r(500, 600)),
                      TokenRange.create(min, tk(300)), r(500, 600));
    }

    @Test
    public void intersectingKeyParticipants()
    {
        // key participants slice to (before(k), k] ranges; these never touch each other, so nothing merges
        TxnRead read = rangeRead(0, 1000);
        RoutingKeys keys = RoutingKeys.of(tk(100), tk(200), tk(300));
        TxnRead sliced = (TxnRead) read.intersecting(keys);
        Ranges expected = ((Ranges) read.keys()).intersecting(keys, Routables.Slice.Minimal);
        assertThat(expected.size()).isEqualTo(3);
        assertRead(sliced, expected.stream().toArray(Range[]::new));
    }

    @Test
    public void slicedSerializedSizeMatches() throws IOException
    {
        // as in StorageProxy.readWithAccord, the txn's keys are the read's keys; Txn.slice / Txn.intersecting slice them
        // separately and they stay unmerged, so the merged read key (100,400] is not one of them
        TxnRead read = rangeRead(0, 1000);
        Ranges touching = ranges(r(100, 200), r(200, 300), r(300, 400));
        TableMetadatasAndKeys txnKeys = new TableMetadatasAndKeys(tables, read.keys());

        TableMetadatasAndKeys slicedKeys = txnKeys.slice(touching);
        assertThat(slicedKeys.keys.size()).isEqualTo(3);
        assertRoundTrips((TxnRead) read.slice(touching), slicedKeys, r(100, 400));

        TableMetadatasAndKeys intersectingKeys = txnKeys.intersecting(touching);
        assertThat(intersectingKeys.keys.size()).isEqualTo(3);
        assertRoundTrips((TxnRead) read.intersecting(touching), intersectingKeys, r(100, 400));
    }

    private static void assertRoundTrips(TxnRead sliced, TableMetadatasAndKeys tablesAndKeys, Range... expected) throws IOException
    {
        assertRead(sliced, expected);
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            Serializers.testSerde(output, TxnRead.serializer, sliced, tablesAndKeys, Version.LATEST);
        }
    }

    @Test
    public void serializedSeekableSizeMatchesBytesWritten() throws IOException
    {
        TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(tables, ranges(r(100, 200), r(200, 300)));
        // r(100, 200) is a member (index + 1 as a vint); r(100, 300) is not (0, then the inline range)
        for (TokenRange range : new TokenRange[] { r(100, 200), r(100, 300) })
        {
            try (DataOutputBuffer output = new DataOutputBuffer())
            {
                tablesAndKeys.serializeSeekable(range, output);
                assertThat(tablesAndKeys.serializedSeekableSize(range)).describedAs("%s", range).isEqualTo(output.getLength());
            }
        }
    }

    private static TxnRead sliceOf(TxnRead read, Range... ranges)
    {
        return (TxnRead) read.slice(ranges(ranges));
    }

    // merge must give the same result whichever side is `this`
    private static void assertMerges(TxnRead a, TxnRead b, Range... expected)
    {
        assertRead(a.merge(b), expected);
        assertRead(b.merge(a), expected);
    }

    @Test
    public void mergeTouchingItems()
    {
        TxnRead read = rangeRead(0, 1000);
        assertMerges(sliceOf(read, r(100, 200)), sliceOf(read, r(200, 300)), r(100, 300));
    }

    @Test
    public void mergeKeepsTails()
    {
        TxnRead read = rangeRead(0, 1000);
        assertMerges(sliceOf(read, r(100, 200)), sliceOf(read, r(100, 200), r(300, 400)), r(100, 200), r(300, 400));
        // the longer side's tail follows the shorter side's last item
        assertMerges(sliceOf(read, r(100, 200), r(500, 600)), sliceOf(read, r(300, 400)), r(100, 200), r(300, 400), r(500, 600));
    }

    @Test
    public void mergeContained()
    {
        TxnRead read = rangeRead(0, 1000);
        assertMerges(sliceOf(read, r(100, 200)), sliceOf(read, r(100, 400)), r(100, 400));
    }

    @Test
    public void mergeOverlappingOffset()
    {
        TxnRead read = rangeRead(0, 1000);
        assertMerges(sliceOf(read, r(100, 300)), sliceOf(read, r(200, 400)), r(100, 400));
    }

    @Test
    public void mergeWithEmptySide()
    {
        TxnRead read = rangeRead(0, 1000);
        TxnRead nonEmpty = sliceOf(read, r(100, 200));
        TxnRead empty = sliceOf(read, r(2000, 3000));
        assertThat(empty.keys().isEmpty()).isTrue();
        for (TxnRead merged : new TxnRead[] { (TxnRead) nonEmpty.merge(empty), (TxnRead) empty.merge(nonEmpty) })
        {
            assertThat(merged).isEqualTo(nonEmpty);
            // the empty read has no tables and no consistency level, so the result must not be built from it
            assertThat(merged.tables).isSameAs(nonEmpty.tables);
            assertThat(merged.cassandraConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
        }
    }

    @Test
    public void mergeTouchingOrOverlappingDifferentNamesFails()
    {
        // items of two different reads that touch or overlap cannot be folded, as the result would drop one read's name
        TxnRead named0 = sliceOf(twoItemRead(0, 1000, 5000, 6000), r(100, 200));
        for (Range other : new Range[] { r(200, 300), r(150, 300) })
        {
            TxnRead named1 = sliceOf(twoItemRead(5000, 6000, 0, 1000), other);
            assertThat(named0.get(0).txnDataName()).isNotEqualTo(named1.get(0).txnDataName());
            // the message names both items, so both names and both ranges
            String first = named0.get(0).toString(), second = named1.get(0).toString();
            assertThatThrownBy(() -> named0.merge(named1)).isInstanceOf(IllegalStateException.class).hasMessageContaining(first).hasMessageContaining(second);
            assertThatThrownBy(() -> named1.merge(named0)).isInstanceOf(IllegalStateException.class).hasMessageContaining(first).hasMessageContaining(second);
        }
    }

    @Test
    public void mergeKeepsNamesOfInterleavedItems()
    {
        // two named items, each sliced twice; the sides interleave but no item of one name touches one of the other
        TxnRead both = twoItemRead(0, 1000, 5000, 6000);
        TxnRead left = sliceOf(both, r(100, 200), r(5100, 5200));
        TxnRead right = sliceOf(both, r(300, 400), r(5300, 5400));
        int name0 = both.get(0).txnDataName(), name1 = both.get(1).txnDataName();
        assertThat(name0).isNotEqualTo(name1);
        for (TxnRead merged : new TxnRead[] { (TxnRead) left.merge(right), (TxnRead) right.merge(left) })
        {
            assertRead(merged, r(100, 200), r(300, 400), r(5100, 5200), r(5300, 5400));
            assertThat(new int[] { merged.get(0).txnDataName(), merged.get(1).txnDataName(), merged.get(2).txnDataName(), merged.get(3).txnDataName() })
                .containsExactly(name0, name0, name1, name1);
        }
    }

    @Test
    public void mergedReadSerializes() throws IOException
    {
        // as PartialTxn.with: the txn keys are the merge of both sides' sliced keys, which keeps touching ranges apart,
        // so the merged read key (100,300] is not one of them
        TxnRead read = rangeRead(0, 1000);
        TableMetadatasAndKeys txnKeys = new TableMetadatasAndKeys(tables, read.keys());
        TableMetadatasAndKeys mergedKeys = txnKeys.slice(ranges(r(100, 200))).merge(txnKeys.slice(ranges(r(200, 300))));
        assertThat(mergedKeys.keys).isEqualTo(ranges(r(100, 200), r(200, 300)));
        assertRoundTrips((TxnRead) sliceOf(read, r(100, 200)).merge(sliceOf(read, r(200, 300))), mergedKeys, r(100, 300));
    }

    @Test
    public void mergeRandomSlicesCoversUnion()
    {
        // each range goes to the left side, the right side or both; touching ranges are common, so sides contain runs
        // that touch, overlap or sit inside a run of the other side. A range is often accompanied by one that overlaps
        // it at an offset (either end) or sits inside it, placed on the same side, the other side or both at random
        Random random = new Random(42);
        TxnRead read = rangeRead(0, 100_000);
        for (int iteration = 0; iteration < 200; iteration++)
        {
            List<Range> left = new ArrayList<>(), right = new ArrayList<>(), all = new ArrayList<>();
            long end = 1;
            for (int count = 1 + random.nextInt(8); count > 0; --count)
            {
                long start = end + random.nextInt(3); // zero touches the previous range
                end = start + 1 + random.nextInt(20);
                Range range = r(start, end);
                all.add(range);
                int side = random.nextInt(3);
                if (side != 1) left.add(range);
                if (side != 0) right.add(range);

                long from, to;
                switch (random.nextInt(4))
                {
                    case 1: // overlaps at the end, starting at or inside this range
                        from = start + random.nextInt((int) (end - start));
                        to = end + 1 + random.nextInt(10);
                        break;
                    case 2: // overlaps at the start, ending inside this range
                        from = Math.max(1, start - 1 - random.nextInt(10));
                        to = start + 1 + random.nextInt((int) (end - start));
                        break;
                    case 3: // inside this range
                        from = start + random.nextInt((int) (end - start));
                        to = from + 1 + random.nextInt((int) (end - from));
                        break;
                    default:
                        continue;
                }
                Range other = r(from, to);
                all.add(other);
                int otherSide = random.nextInt(3);
                if (otherSide != 1) left.add(other);
                if (otherSide != 0) right.add(other);
            }
            Range[] expected = Ranges.of(all.toArray(new Range[0])).mergeTouching().stream().toArray(Range[]::new);
            assertMerges(sliceOf(read, left.toArray(new Range[0])), sliceOf(read, right.toArray(new Range[0])), expected);
        }
    }
}
