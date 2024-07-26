package com.cogwheel.externalsort;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.URLUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class FastLongBaseExternalSortTest {
	
	private static final Logger log = LoggerFactory.getLogger(FastLongBaseExternalSortTest.class);


	
	@Test
	public void sortAll() throws Exception {
		final StopWatch stopWatch = new StopWatch();
		final String workDir = filePath("sortAll");
		AtomicInteger num = new AtomicInteger(0);
		AtomicLong atomicLong = new AtomicLong(0);
		int totalBatch = 10000_0000;


		final String a = "a";
		final String b = "b";

		Map<String, Long> map = new HashMap<>();
		map.put(a, null);
		map.put(b, null);

		FastLongBaseExternalSort<String> externalSort = new FastLongBaseExternalSort<>(
				new File(workDir),
				map.keySet(),
				Comparator.comparingLong(c -> c[1]),
				50 * 1024 * 1024
		);
		try {
			stopWatch.start("build unsorted file");
			log.info(stopWatch.currentTaskName());

			LongStream.range(0, totalBatch).forEach(i -> {
				map.put(a, i);
				map.put(b, totalBatch - i - 1);
				externalSort.appendLine(map);
				map.clear();
			});
			stopWatch.stop();


			stopWatch.start("forEachForTest file");
			log.info(stopWatch.currentTaskName());
			externalSort.forEachForTest(line -> num.incrementAndGet());
			log.info("{}", num.get());
			stopWatch.stop();

			stopWatch.start("check unsorted file");
			log.info(stopWatch.currentTaskName());
			externalSort.forEachForTest(line ->
					Assert.isTrue(
							line.get(0) + line.get(1) == totalBatch - 1,
							() -> new RuntimeException(
									String.format(
											"content is not correct %s atomic:%s",
											join(line),
											atomicLong.get())
							)

					));
			stopWatch.stop();
			num.set(0);

			stopWatch.start("sort");
			log.info(stopWatch.currentTaskName());
			externalSort.sortAll();
			stopWatch.stop();

			stopWatch.start("check sorted file");
			log.info(stopWatch.currentTaskName());
			atomicLong.set(0);
			externalSort.forEachSorted(line ->
					check(line, totalBatch, atomicLong, 1)
			);
			stopWatch.stop();
			externalSort.close();
			externalSort.deleteOutFile();
			atomicLong.set(0);
			stopWatch.start("foreach and check after close");
			log.info(stopWatch.currentTaskName());
			FastLongBaseExternalSort.forEachSorted(new File(workDir), 2, line -> check(line, totalBatch, atomicLong, 1));
			stopWatch.stop();
			/* please del workDir if not use anymore */
			FileUtil.del(workDir);
		}
		finally {
			log.info(stopWatch.prettyPrint(TimeUnit.MILLISECONDS));
		}
	}

	public static void check(List<Long> line, int totalBatch, AtomicLong atomicLong, int idx) {
		boolean condition = atomicLong.getAndIncrement() == line.get(idx) && line.get(0) + line.get(1) == totalBatch - 1;
		if (!condition) {
			throw new RuntimeException(String.format("result is not correct: %s atomic:%s", join(line), atomicLong.get()));
		}
	}

	private static String join(List<?> line) {
		return line.stream().map(String::valueOf).collect(Collectors.joining(","));
	}

	@SuppressWarnings("all")
	private static String filePath(String methodName) {
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
