package com.gearwheel.externalsort;

import cn.hutool.core.io.FileUtil;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

/**
 * PerformanceTest
 *
 * @date 2024-09-18 16:41
 */
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 3)
public class PerformanceTest {

    private static final Logger log = LoggerFactory.getLogger(PerformanceTest.class);

    private static String workDirUsingArray;
    private static String workDirUsingList;

    private static final int totalBatch = 100_000_000;

    private static final String a = "a";
    private static final String b = "b";
    private static  Map<String, Long> map;;

    TestReadyEntityUsingList testReadyEntityUsingList;
    TestReadyEntityUsingArray testReadyEntityUsingArray;


    @Setup(value = Level.Invocation)
    public void Setup() {
        String threadName = Thread.currentThread().getName();
//        log.error("threadName:{}", threadName);
        if (threadName.contains("List")) {
            usingListSetup();
        }
        else {
            usingArraySetup();
        }
    }

    @TearDown(value = Level.Invocation)
    public void TearDown() {
        String threadName = Thread.currentThread().getName();
//        log.error("threadName:{}", threadName);

        if (threadName.contains("List")) {
            FileUtil.del(workDirUsingList);
        }
        else {
            FileUtil.del(workDirUsingArray);
        }
    }

    private void usingListSetup() {
        map = new HashMap<String, Long>(){{
            put(a, 1L);
            put(b, 1L);
        }};
        workDirUsingList = TestUtil.filePath("usingList");

        testReadyEntityUsingList = new TestReadyEntityUsingList();

        LongStream.range(0, totalBatch).forEach(i -> {
            map.put(a, i);
            map.put(b, totalBatch - i - 1);
            testReadyEntityUsingList.externalSort.appendLine(map);
            map.clear();
        });
        testReadyEntityUsingList.externalSort.forEachForTest(line -> TestUtil.assertTrue(line, totalBatch, testReadyEntityUsingList.atomicLong));

    }

    private void usingArraySetup() {
        map = new HashMap<String, Long>(){{
            put(a, 1L);
            put(b, 1L);
        }};
        workDirUsingArray = TestUtil.filePath("usingArray");

        testReadyEntityUsingArray = new TestReadyEntityUsingArray();

        LongStream.range(0, totalBatch).forEach(i -> {
            map.put(a, i);
            map.put(b, totalBatch - i - 1);
            testReadyEntityUsingArray.externalSort.appendLine(map);
            map.clear();
        });
        testReadyEntityUsingArray.externalSort.forEachForTest(line -> TestUtil.assertTrue(line, totalBatch, testReadyEntityUsingArray.atomicLong));
    }


    @Benchmark
    public void sortUsingArray() throws Exception {

        FastLongBaseExternalSort<String> externalSort = testReadyEntityUsingArray.externalSort;
        externalSort.sortAll();

        AtomicLong atomicLong = testReadyEntityUsingArray.atomicLong;
        atomicLong.set(0);
        externalSort.forEachSorted(line -> TestUtil.check(line, totalBatch, atomicLong, 1));
        externalSort.close();
        externalSort.deleteOutFile();
        atomicLong.set(0);
        /* foreach and check after close */
        FastLongBaseExternalSort.forEachSorted(new File(workDirUsingArray), 2, line -> TestUtil.check(line, totalBatch, atomicLong, 1));

    }

    @Benchmark
    public void sortUsingList() throws Exception {


        UsingListFastLongBaseExternalSort<String> externalSort = testReadyEntityUsingList.externalSort;
        externalSort.sortAll();

        AtomicLong atomicLong = testReadyEntityUsingList.atomicLong;
        atomicLong.set(0);
        externalSort.forEachSorted(line -> TestUtil.check(line, totalBatch, atomicLong, 1));
        externalSort.close();
        externalSort.deleteOutFile();
        atomicLong.set(0);
        /* foreach and check after close */
        FastLongBaseExternalSort.forEachSorted(new File(workDirUsingList), 2, line -> TestUtil.check(line, totalBatch, atomicLong, 1));

    }



    private static class TestReadyEntityUsingList {
        AtomicLong atomicLong = new AtomicLong(0);
        UsingListFastLongBaseExternalSort<String> externalSort;

        public TestReadyEntityUsingList() {
            externalSort = new UsingListFastLongBaseExternalSort<>(
                    new File(workDirUsingList),
                    map.keySet(),
                    Comparator.comparingLong(c -> c[1]),
                    50 * 1024 * 1024
            );
        }
    }


    private static class TestReadyEntityUsingArray {
        AtomicLong atomicLong = new AtomicLong(0);
        FastLongBaseExternalSort<String> externalSort;

        public TestReadyEntityUsingArray() {
            externalSort = new FastLongBaseExternalSort<>(
                    new File(workDirUsingArray),
                    map.keySet(),
                    Comparator.comparingLong(c -> c[1]),
                    50 * 1024 * 1024
            );
        }
    }



    @Test
    public void test1() throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(this.getClass().getSimpleName())
                .build();
        new Runner(opt).run();

        /*
            Benchmark                       Mode  Cnt      Score       Error  Units
            PerformanceTest.sortUsingArray  avgt    3  10476.764 ± 19691.874  ms/op
            PerformanceTest.sortUsingList   avgt    3  11332.628 ± 41537.923  ms/op
        */
    }
}
