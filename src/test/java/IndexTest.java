import com.codepoetics.joink.Index;
import com.codepoetics.joink.JoinKey;
import com.codepoetics.joink.Tuple2;
import org.junit.Test;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexTest {

    private static class Item {
        private static int lastId = 0;
        public final int id;
        public final int joinOn;
        public Item() {
            id = lastId++;
            joinOn = (int) (Math.random() * 100);
        }
        @Override public String toString() { return Integer.toString(id) + ": " + Integer.toString(joinOn); }
    }

    JoinKey<Item, Integer> itemId = i -> i.joinOn;

    @Test public void
    performance_test() {
        List<Item> items1 = new LinkedList<>();
        List<Item> items2 = new LinkedList<>();

        for (int i=0; i < 1000000; i++) {
            items1.add(new Item());
            items2.add(new Item());
        }

        Instant start = Instant.now();
        Index<Integer, Item> index1 = itemId.index(items1);
        Instant firstIndex = Instant.now();
        Index<Integer, Item> index2 = itemId.index(items2);
        Instant secondIndex = Instant.now();
        Stream<Tuple2<Item, Item>> innerJoined = index1.manyToOne(index2);
        Instant joined = Instant.now();
        List<Tuple2<Item, Item>> collected = innerJoined.collect(Collectors.toList());
        Instant finished = Instant.now();
        System.out.println(collected.size());

        System.out.println(String.format("Started: %s\nFirst index: %s\nSecond index: %s\n Joined: %s\n Collected: %s",
                start,
                firstIndex.toEpochMilli() - start.toEpochMilli(),
                secondIndex.toEpochMilli() - firstIndex.toEpochMilli(),
                joined.toEpochMilli() - secondIndex.toEpochMilli(),
                finished.toEpochMilli() - joined.toEpochMilli()));
    }

}
