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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.Routable.Domain;
import accord.primitives.Seekable;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AbstractKeySortedTest
{
    private static final TableId TABLE1 = TableId.fromString("00000000-0000-0000-0000-000000000001");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    static class Item
    {
        final PartitionKey key;
        final int value;

        public Item(PartitionKey key, int value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return value == item.value && key.equals(item.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, value);
        }

        @Override
        public String toString()
        {
            return "Item{" +
                   "key=" + key +
                   ", value=" + value +
                   '}';
        }
    }

    static class SortedItems extends AbstractKeySorted<Item>
    {
        public SortedItems(Item... items)
        {
            super(items, Domain.Key);
        }

        public SortedItems(List<Item> items)
        {
            super(items, Domain.Key);
        }

        @Override
        int compareNonKeyFields(Item left, Item right)
        {
            return Integer.compare(left.value, right.value);
        }

        @Override
        Seekable getKey(Item item)
        {
            return item.key;
        }

        @Override
        Item[] newArray(int size)
        {
            return new Item[size];
        }
    }

    static class RangeItem
    {
        final TokenRange key;
        final int value;

        public RangeItem(TokenRange key, int value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RangeItem item = (RangeItem) o;
            return value == item.value && key.equals(item.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, value);
        }

        @Override
        public String toString()
        {
            return "RangeItem{key=" + key + ", value=" + value + '}';
        }
    }

    static class SortedRanges extends AbstractKeySorted<RangeItem>
    {
        public SortedRanges(RangeItem... items)
        {
            super(items, Domain.Range);
        }

        @Override
        int compareNonKeyFields(RangeItem left, RangeItem right)
        {
            return Integer.compare(left.value, right.value);
        }

        @Override
        Seekable getKey(RangeItem item)
        {
            return item.key;
        }

        @Override
        RangeItem[] newArray(int size)
        {
            return new RangeItem[size];
        }
    }

    private static PartitionKey key(int k)
    {
        DecoratedKey dk = ByteOrderedPartitioner.instance.decorateKey(ByteBufferUtil.bytes(k));
        return new PartitionKey(TABLE1, dk);
    }

    private static TokenRange range(long start, long end)
    {
        return TokenRange.create(TABLE1, new LongToken(start), new LongToken(end));
    }

    private static RangeItem rangeItem(long start, long end, int v)
    {
        return new RangeItem(range(start, end), v);
    }

    private static Item item(int k, int v)
    {
        return new Item(key(k), v);
    }

    private static List<Item> itemList(Item... items)
    {
        return Lists.newArrayList(items);
    }

    @Test
    public void checkInitialSorting()
    {
        List<Item> initial = itemList(item(5, 4), item(3, 3), item(3, 1), item(6, 5));
        SortedItems expected = new SortedItems(item(3, 1), item(3, 3), item(5, 4), item(6, 5));
        expected.validateOrder();
        SortedItems actual = new SortedItems(initial);
        actual.validateOrder();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void checkIterationForKey()
    {
        SortedItems source = new SortedItems(item(1, 5), item(3, 1), item(3, 3), item(5, 4), item(6, 5));
        source.validateOrder();

        source.forEachWithKey(key(0), i -> Assert.fail());
        source.forEachWithKey(key(1), i -> Assert.assertEquals(item(1, 5), i));
        source.forEachWithKey(key(2), i -> Assert.fail());
        List<Item> actual = new ArrayList<>();
        source.forEachWithKey(key(3), actual::add);
        Assert.assertEquals(itemList(item(3, 1), item(3, 3)), actual);
        source.forEachWithKey(key(4), i -> Assert.fail());
        source.forEachWithKey(key(5), i -> Assert.assertEquals(item(5, 4), i));
        source.forEachWithKey(key(6), i -> Assert.assertEquals(item(6, 5), i));
        source.forEachWithKey(key(7), i -> Assert.fail());
    }

    /**
     * Reproduces the inverted comparator in AbstractKeySorted#firstPossibleRangeIdx
     * (src/java/org/apache/cassandra/service/accord/txn/AbstractKeySorted.java:226-239).
     * The existing checkIterationForKey test only exercises Domain.Key, which uses the
     * (correct) firstPossibleKeyIdx path, masking this bug.
     *
     * TRIGGER: 3 non-overlapping, already-sorted TokenRanges. Looking up any range beyond
     *          the first forces the binary search to walk right past index 0, which is the
     *          direction the inverted comparator gets wrong.
     * HARNESS: SortedRanges (Domain.Range) built via the array constructor, JUnit.
     * ORACLE:  forEachWithKey(lastRange, ...) must visit exactly the item keyed by lastRange.
     */
    @Test
    public void checkIterationForRange()
    {
        RangeItem r0 = rangeItem(0, 10, 1);
        RangeItem r1 = rangeItem(20, 30, 2);
        RangeItem r2 = rangeItem(40, 50, 3);
        SortedRanges source = new SortedRanges(r0, r1, r2);
        source.validateOrder();

        List<RangeItem> actual = new ArrayList<>();
        source.forEachWithKey(r2.key, actual::add);
        // Correct behavior: the last range's own lookup must find itself.
        // Buggy behavior: firstPossibleRangeIdx's inverted comparator sends the binary
        // search left instead of right, landing on index 0 and finding nothing.
        Assert.assertEquals(Lists.newArrayList(r2), actual);
    }
}
