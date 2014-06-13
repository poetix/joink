package com.codepoetics.joink;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexTest {

    @PerfTest(invocations = 100, threads = 1)
    @Test
    public void
    performance_test_much_data() {
        joinAndCollect(largeDataLeft, largeDataRight);
    }

    @PerfTest(invocations = 1000, threads = 1)
    @Test
    public void
    performance_test_little_data() {
        joinAndCollect(smallDataLeft, smallDataRight);
    }

    private static class Item {
        private static int lastId = 0;
        public final int id;
        public final int joinOn;

        public Item(int keyRange) {
            id = lastId++;
            joinOn = (int) (Math.random() * keyRange);
        }

        @Override
        public String toString() {
            return Integer.toString(id) + ": " + Integer.toString(joinOn);
        }
    }

    @Before
    public void setup() {
        singleThreadedPool = new ForkJoinPool(1);
        for (int i = 0; i < 10000; i++) {
            largeDataLeft.add(new Item(100));
            largeDataRight.add(new Item(100));
        }

        for (int i = 0; i < 1000; i++) {
            smallDataLeft.add(new Item(10));
            smallDataRight.add(new Item(10));
        }
    }

    @After
    public void shutdownPool() {
        singleThreadedPool.shutdown();
    }

    @Rule
    public ContiPerfRule rule = new ContiPerfRule();

    private void joinAndCollect(Collection<Item> left, Collection<Item> right) {
        Index<Integer, Item> index1 = itemId.index(left);
        Index<Integer, Item> index2 = itemId.index(right);
        Stream<Tuple2<Item, Item>> innerJoined = index1.manyToOne(index2);
        List<Tuple2<Item, Item>> collected = innerJoined.collect(Collectors.toList());
    }

    private final JoinKey<Item, Integer> itemId = i -> i.joinOn;

    private final List<Item> largeDataLeft = new LinkedList<>();
    private final List<Item> largeDataRight = new LinkedList<>();

    private final List<Item> smallDataLeft = new LinkedList<>();
    private final List<Item> smallDataRight = new LinkedList<>();

    private ForkJoinPool singleThreadedPool;
}
