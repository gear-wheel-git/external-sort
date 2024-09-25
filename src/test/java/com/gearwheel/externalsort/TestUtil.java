package com.gearwheel.externalsort;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.URLUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * TestUtil
 */
public class TestUtil {


    static void assertTrue(long[] line, int totalBatch, AtomicLong atomicLong) {
        Assert.isTrue(
                line[0] + line[1] == totalBatch - 1,
                () -> new RuntimeException(
                        String.format(
                                "content is not correct %s atomic:%s",
                                join(line),
                                atomicLong.get())
                )

        );
    }

    static void assertTrue(List<Long> line, int totalBatch, AtomicLong atomicLong) {
        Assert.isTrue(
                line.get(0) + line.get(1) == totalBatch - 1,
                () -> new RuntimeException(
                        String.format(
                                "content is not correct %s atomic:%s",
                                join(line),
                                atomicLong.get())
                )

        );
    }

   static void check(List<Long> line, int totalBatch, AtomicLong atomicLong, int idx) {
        boolean condition = atomicLong.getAndIncrement() == line.get(idx) && line.get(0) + line.get(1) == totalBatch - 1;
        if (!condition) {
            throw new RuntimeException(String.format("result is not correct: %s atomic:%s", join(line), atomicLong.get()));
        }
    }

   static void check(long[] line, int totalBatch, AtomicLong atomicLong, int idx) {
        boolean condition = atomicLong.getAndIncrement() == line[idx] && line[0] + line[1] == totalBatch - 1;
        if (!condition) {
            throw new RuntimeException(String.format("result is not correct: %s atomic:%s", join(line), atomicLong.get()));
        }
    }

    static String join(List<?> line) {
        return line.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    static String join(long[] line) {
        return Arrays.stream(line).mapToObj(String::valueOf).collect(Collectors.joining(","));
    }

    @SuppressWarnings("all")
    static String filePath(String methodName) {
        String filePath = FileUtil.normalize(
                URLUtil.decode(
                        FastLongBaseExternalSortTest.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .getPath(),
                        StandardCharsets.UTF_8.name())
                        + "/" + methodName);
        FileUtil.mkdir(filePath);
        return filePath;
    }
}
