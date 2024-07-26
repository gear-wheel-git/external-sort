package com.gearwheel.externalsort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * External sorting <br> of equal length per row for long type <br>
 * <p color="#51D138">
 *     The output file will be preserved after sorted
 *     <br>
 *     please del workDir if not use anymore
 * </p>
 * <br>
 * <strong>when running on my pc </strong>
 * <p>
 * 100 million data sorting: almost 150M or so memory, 14s sorting with g1gc, 63s if not, almost 7s sorting if the memory is a little more
 * </p>
 * <p>
 * 1 billion data sorting 293s sorting completed (not in-depth test)
 * </p>
 *
 * @author gear-wheel
 */
public final class FastLongBaseExternalSort<H> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FastLongBaseExternalSort.class);

    private final Map<H, Integer> head;

    private final List<long[]> memoryStorage;
    private final int diskLineNum;
    private final Comparator<long[]> comparator;

    private final LongBaseStorage in;
    private LongBaseStorage out;


    /**
     * 
     * @param workdir  this directory  should be empty
     * @param heads just use for append content to get idx of line 
     * @param comparator comparator of line content
     * @param segmentSizeBytes determine how large disk file blocks are created and how much content is read into memory
     */
    public FastLongBaseExternalSort(File workdir,
                                    Collection<H> heads,
                                    Comparator<long[]> comparator,
                                    int segmentSizeBytes) {
        if (workdir == null || !workdir.isDirectory()) {
            throw new IllegalArgumentException("input File object is not a directory");
        }

        this.in = new LongBaseStorage(workdir, "data", segmentSizeBytes, false);
        this.head = createHeadMap(heads);
        this.diskLineNum = segmentSizeBytes / (head.size() * Long.BYTES);
        this.memoryStorage = new ArrayList<>(diskLineNum);
        this.comparator = comparator;
        log.info("may use {} at least ", ExternalSortUtils.format(memorySize()));
    }

    /**
     * foreach of exist sorted file
     *
     * @param directory target dir
     * @param headSize  head size
     * @param action    consumer
     * @throws Exception ex
     */
    public static void forEachSorted(File directory, int headSize, Consumer<List<Long>> action) throws Exception {
        try (LongBaseStorage longBaseStorage = new LongBaseStorage(directory, "out", 0, true)) {
            doForEach(longBaseStorage, headSize, action);
        }
    }

    private static void doForEach(LongBaseStorage storage, int lineSize, Consumer<List<Long>> action) {
        Objects.requireNonNull(action);

        List<Long> list = new ArrayList<>(lineSize);
        storage.forEach(l -> {
            list.add(l);
            if (list.size() >= lineSize) {
                action.accept(list);
                list.clear();
            }
        });
    }

    /**
     * Estimated memory size required
     * @return bytes
     */
    public long memorySize() {
        return diskLineNum * (8L * head.size() + 16 + 4) + 40;
    }

    private Map<H, Integer> createHeadMap(Collection<H> columnNames) {
        Map<H, Integer> headIdxMap = new HashMap<>(columnNames.size());
        int i = 0;
        for (H columnName : columnNames) {
            headIdxMap.put(columnName, i++);
        }
        return headIdxMap;
    }

    /**
     * append line

     * @param head2RowMap it's best to reuse this input map
     */
    public void appendLine(Map<H, Long> head2RowMap) {
        if (head2RowMap == null || head2RowMap.isEmpty()) {
            return;
        }

        long[] longs = new long[head.size()];
        // build a line order by head
        for (Map.Entry<H, Integer> headName2Idx : head.entrySet()) {
            longs[headName2Idx.getValue()] = head2RowMap.get(headName2Idx.getKey());
        }
        memoryStorage.add(longs);

        if (memoryStorage.size() >= diskLineNum) {
            sortAndFlush(true);
        }
    }

    /**
     * append multi line
     * @param head2RowListMap head  to row list map
     */
    public void append(Map<H, List<Long>> head2RowListMap) {
        if (head2RowListMap == null || head2RowListMap.isEmpty()) {
            return;
        }
        int size = 0;
        List<Long> list;
        for (Map.Entry<H, List<Long>> headName2RowListEntry : head2RowListMap.entrySet()) {
            list = headName2RowListEntry.getValue();

            if (size != 0 && list.size() != size) {
                throw new IllegalArgumentException("inserts multiple columns, but the number of rows is not the same");
            }
            size = list.size();
        }
        for (int i = 0; i < size; i++) {
            long[] longs = new long[head.size()];
            // build a line order by head
            for (Map.Entry<H, Integer> headName2Idx : head.entrySet()) {
                longs[headName2Idx.getValue()] = head2RowListMap.get(headName2Idx.getKey()).get(i);
            }
            memoryStorage.add(longs);

            if (memoryStorage.size() >= diskLineNum) {
                sortAndFlush(true);
            }
        }
    }

    private void sortAndFlush(boolean createNewSegment) {
        memoryStorage.sort(comparator);

        flush(in);

        if (createNewSegment) {
            in.createNewSegment();
        }
    }

    private void flush(LongBaseStorage storage) {
        final List<long[]> bucket = memoryStorage;

        bucket.forEach(line -> {
            for (long l : line) {
                storage.putLongUncheck(l);
            }
        });

        bucket.clear();
        storage.commit();
    }

    /**
     * just for test
     * <p>
     *     because sorting occurs during insertion, this method cannot guarantee that the order is consistent with insertion.
     * </p>
     * @param action consumer
     */
    public void forEachForTest(Consumer<List<Long>> action) {
        sortAndFlush(false);

        doForEach(in, head.size(), action);
    }

    public void forEachSorted(Consumer<List<Long>> action) {
        
        if (out == null) {
            throw new RuntimeException(" please use sortAll first !");
        }

        doForEach(out, head.size(), action);
    }

    private long[] readLine(ByteBuffer byteBuffer) {

        long[] longs = new long[head.size()];

        for (int i = 0; i < head.size(); i++) {
            longs[i] = byteBuffer.getLong();
        }
        return longs;
    }

    public void sortAll() {
        // flush
        sortAndFlush(false);

        final int outFileBytes = (int) Math.min(in.getCurrentSize(), Integer.MAX_VALUE);

        final int outMaxLineNum = outFileBytes / (head.size() * Long.BYTES);

        this.out = new LongBaseStorage(in.directory, "out", outFileBytes, false);


        final List<Segment.BufferedSegmentReader<long[]>> segments = in.segments.stream().map(x -> x.bufferedReader(this::readLine)).peek(Segment.BufferedSegmentReader::loadNewElement).collect(Collectors.toList());
        Comparator<Segment.BufferedSegmentReader<long[]>> segmentComparator = (o1, o2) -> {
            long[] x = o1.read();
            long[] y = o2.read();
            return comparator.compare(x, y);
        };

        PriorityQueue<Segment.BufferedSegmentReader<long[]>> segmentQueue = new PriorityQueue<>(segments.size(), segmentComparator);
        segmentQueue.addAll(segments);

        long currentOutNum = 0L;

        while (!segmentQueue.isEmpty()) {
            Segment.BufferedSegmentReader<long[]> poll = segmentQueue.poll();

            memoryStorage.add(poll.read());

            poll.loadNewElement();

            if (poll.read() != null) {
                segmentQueue.add(poll);
            }

            if (memoryStorage.size() >= diskLineNum) {
                currentOutNum += memoryStorage.size();

                if (currentOutNum >= outMaxLineNum) {
                    out.createNewSegment();
                    currentOutNum = 0L;
                }
                flush(out);
            }
        }


        memoryStorage.removeIf(Objects::isNull);
        flush(out);
    }


    public void deleteOutFile() {
        if (out != null) {
            out.clear();
        }
    }

    @Override
    public void close() throws Exception {
        if (out != null) {
            out.close();
        }

        in.clear();
        in.close();
    }

}
