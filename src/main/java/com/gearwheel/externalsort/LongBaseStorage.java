package com.gearwheel.externalsort;


import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 * @author gear-wheel
 * @date 2024-07-15 10:12
 */
@SuppressWarnings({"unused"})
class LongBaseStorage extends BaseDataStorage {

    public LongBaseStorage(File directory, String prefix, int segmentSizeBytes, boolean allowLoad) {
        super(directory, prefix, segmentSizeBytes, allowLoad);
    }


    @SuppressWarnings("all")
    public void forEach(Consumer<Long> action) {
        final List<Integer> idxList = readIdxList();
        final List<Segment> segmentList = this.segments;
        int pos;
        Segment segment;
        for (int i = 0; i < idxList.size(); i++) {
            pos = idxList.get(i);
            segment = segmentList.get(i);

            ByteBuffer bb = segment.bb.asReadOnlyBuffer();
            bb.flip();
            while (bb.position() < pos) {
                action.accept(bb.getLong());
            }
        }
    }

    @SuppressWarnings("all")
    public void forEach(int lenOfArray, Consumer<long[]> action) {
        final List<Integer> idxList = readIdxList();
        final List<Segment> segmentList = this.segments;
        int pos;
        Segment segment;
        long[] array = new long[lenOfArray];
        int cIdx = 0;
        for (int i = 0; i < idxList.size(); i++) {
            pos = idxList.get(i);
            segment = segmentList.get(i);

            ByteBuffer bb = segment.bb.asReadOnlyBuffer();
            bb.flip();
            while (bb.position() < pos) {
                array[cIdx] = bb.getLong();
                cIdx++;
                if (cIdx >= lenOfArray) {
                    action.accept(array);
                    cIdx = 0;
                }
            }
        }
    }

    public void putLong(long l) {
        if (currentSegment.bb.position() + Long.BYTES > segmentSizeBytes) {
            createNewSegment();
            currentSegment = segments.get(segments.size() - 1);
        }
        currentSegment.bb.putLong(l);
    }

    public void putLongUncheck(long l) {
        segments.get(segments.size() - 1).bb.putLong(l);
    }

}
