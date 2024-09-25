package com.gearwheel.externalsort;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author gear-wheel
 * @date 2024-09-18
 */
@SuppressWarnings({"ResultOfMethodCallIgnored", "unused"})
abstract class BaseDataStorage implements AutoCloseable {
    protected static final String FILENAME_IDX = ".idx";
    protected static final String FILENAME_FLAG_DATA = "-";
    protected final File directory;
    protected final String prefix;
    protected final int segmentSizeBytes;
    protected final List<Segment> segments = new ArrayList<>();
    protected final Segment indexSegment;
    protected Segment currentSegment;


    public BaseDataStorage(File directory, String prefix, int segmentSizeBytes, boolean allowLoad) {
        if (directory == null || !directory.isDirectory()) {
            throw new IllegalArgumentException(" directory is illegal " + directory);
        }
        // file from disk
        File[] files = directory.listFiles();
        File idxFile = new File(directory, prefix + FILENAME_IDX);

        if (!allowLoad) {
            del(idxFile);
            if (files != null) {
                Arrays.stream(files).filter(x -> x.getName().startsWith(prefix + FILENAME_FLAG_DATA)).forEach(BaseDataStorage::del);
            }
        }

        this.directory = directory;
        this.prefix = prefix;
        this.indexSegment = map(idxFile, 0, Integer.BYTES * 1024 * 1024);


        List<Integer> idxList;

        boolean canLoadOld = allowLoad && idxFile.exists() && files != null && Arrays.stream(files).anyMatch(x -> x.getName().contains(prefix + FILENAME_FLAG_DATA));
        if (canLoadOld && !(idxList = readIdxList()).isEmpty()) {

            this.segmentSizeBytes = (int) new File(directory, prefix + FILENAME_FLAG_DATA + 0).length();

            for (int i = 0; i < idxList.size(); i++) {
                createSegment(i, idxList.get(i));
            }

        } else {
            // init params
            this.segmentSizeBytes = segmentSizeBytes;
        }


        if (segments.isEmpty()) {
            currentSegment = createSegment(0, 0);
        } else {
            currentSegment = segments.get(segments.size() - 1);
        }
    }

    protected static Segment map(File file, long pos, int segmentSizeBytes) {
        try {
            checkFile(file, segmentSizeBytes);
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(segmentSizeBytes);
            }
            FileChannel channel = (FileChannel) Files.newByteChannel(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            // map the whole file
            MappedByteBuffer bb = channel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSizeBytes);
            // setPos
            bb.position((int) pos);
            return new Segment(file, channel, bb);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected static void checkFile(File file, int segmentSizeBytes) {
        if (file.exists() && file.length() != segmentSizeBytes) {
            throw new IllegalStateException("segment file " + file + " should be of size " + segmentSizeBytes + " but was of size " + file.length());
        }
    }

    protected static void del(File file) {
        if (file.exists()) {
            file.delete();
        }
    }

    protected Segment createSegment(long idx, long pos) {
        File file = new File(directory, prefix + FILENAME_FLAG_DATA + idx);
        Segment segment = map(file, pos, segmentSizeBytes);
        segments.add(segment);
        return segment;
    }

    public void createNewSegment() {
        createSegment(segments.size(), 0);
    }

    public void commit() {
        List<Integer> idxList = new ArrayList<>();
        for (Segment segment : segments) {
            segment.bb.force();
            idxList.add(segment.bb.position());
        }

        commitIdx(idxList);
    }

    @Override
    public void close() throws Exception {
        closeSegments();
        indexSegment.close();
    }

    public void clear() {
        /* data file */
        if (!segments.isEmpty()) {
            closeSegments();
            deleteSegmentFiles();
            segments.clear();
        }
        /* index file  */
        try {
            indexSegment.close();
            indexSegment.file.delete();
        } catch (Exception ignore) {
        }

    }

    protected void deleteSegmentFiles() {
        for (Segment segment : segments) {
            segment.file.delete();
        }
    }

    protected void closeSegments() {
        for (Segment segment : segments) {
            try {
                segment.close();
            } catch (Exception ignore) {
            }
        }
    }

    protected List<Integer> readIdxList() {
        ByteBuffer rbb = indexSegment.bb.asReadOnlyBuffer();
        rbb.position(0);
        List<Integer> idxList = new ArrayList<>();
        int idx;
        while ((idx = rbb.getInt()) != 0) {
            idxList.add(idx);
        }

        return idxList;
    }

    public long getCurrentSize() {
        return readIdxList().stream().mapToLong(Integer::longValue).reduce(Long::sum).orElse(0L);
    }

    protected void commitIdx(List<Integer> idxList) {
        MappedByteBuffer mappedByteBuffer = indexSegment.bb;
        mappedByteBuffer.clear();
        idxList.forEach(mappedByteBuffer::putInt);
        mappedByteBuffer.force();
    }
}
