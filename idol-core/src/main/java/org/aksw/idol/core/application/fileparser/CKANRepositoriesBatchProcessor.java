/**
 * 
 */
package org.aksw.idol.core.application.fileparser;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.aksw.idol.core.parsers.descriptionFileParser.DescriptionFileParserLoader;
import org.aksw.idol.core.parsers.descriptionFileParser.Impl.CKANParserIMPL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class CKANRepositoriesBatchProcessor {

	final static Logger logger = LoggerFactory.getLogger(CKANRepositoriesBatchProcessor.class);

	// number of concurrent request to be made for each repository
	final int numberOfConcurrentRequests = 1;

	// number of repositories to be analyzed concurrently
	final int numberOfConcurrentRepositories = 1;


	public void loadAllRepositories(List<String> ckanRepositories, CKANParserIMPL parserImpl) {

		ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentRepositories);
		ckanRepositories.forEach((repo) -> {
			executor.execute(new HttpRepositoryRequestThread(repo, parserImpl));
		});

		executor.shutdown();
		try {
			executor.awaitTermination(300, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("Loaded all CKAN repositories.");

	}

	/**
	 * Load CKAN repositories concurrently
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Oct 1, 2016
	 */
	class HttpRepositoryRequestThread implements Runnable {

		CKANParserIMPL ckanParser;

		// CkanClient client;

		public HttpRepositoryRequestThread(String repository, CKANParserIMPL parserImpl) {
			this.ckanParser = parserImpl;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			DescriptionFileParserLoader parser = new DescriptionFileParserLoader();
			if (!parser.load(ckanParser))
				parser.parse();
		}
	}

}
