/**
 * 
 */
package org.aksw.idol.executors;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 3, 2016
 */
@FunctionalInterface
public interface LodVaderExecutor {
	List<String> executor(String url);

	int maxNumberOfThreads = 15;
	ExecutorService executor = Executors.newFixedThreadPool(maxNumberOfThreads);

	default Future<List<String>> execute(String url) {
		Future<List<String>> s = executor.submit(() -> {
			return executor(url);
		});
		return s;

	}

	default void shutdown() {
		executor.shutdown();
		try {
			executor.awaitTermination(10, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
