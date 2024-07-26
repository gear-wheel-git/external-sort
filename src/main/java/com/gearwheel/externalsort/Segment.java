package com.gearwheel.externalsort;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Function;

/**
 *
 * @author gear-wheel
 * @date 2024-07-15 10:42
 */
class Segment {
    private final FileChannel channel;
    final MappedByteBuffer bb;
    final File file;


    Segment(File file, FileChannel channel, MappedByteBuffer bb) {
        this.file = file;
        this.channel = channel;
        this.bb = bb;
    }

    public void close() throws IOException {
        closeDirectBuffer(bb);
        channel.close();
    }

    public <T> BufferedSegmentReader<T> bufferedReader(Function<ByteBuffer, T> reader) {
        return new BufferedSegmentReader<>(this, reader);
    }

    static class BufferedSegmentReader<T> {

        private final ByteBuffer byteBuffer;

        private T buffer;

        private final Function<ByteBuffer, T> reader;

        public BufferedSegmentReader(Segment segment, Function<ByteBuffer, T> reader) {
            byteBuffer = segment.bb.asReadOnlyBuffer();
            this.reader = reader;
            byteBuffer.flip();
        }

        public void loadNewElement() {
            if (!byteBuffer.hasRemaining()) {
                buffer  = null;
            }
            else {
                buffer = reader.apply(byteBuffer);
            }
        }

        public T read() {
            return buffer;
        }

    }

    public static void closeDirectBuffer(ByteBuffer cb) {
        if (cb == null || !cb.isDirect()) {
            return;
        }

        ExternalSortUtils.cleanDirectBuffer(cb);
    }
}
