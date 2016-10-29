/**
 * 
 */
package lodVader.application.fileparser;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CKANParserIMPL;;

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


	public void loadAllRepositories(List<String> ckanRepositories) {

		ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentRepositories);
		ckanRepositories.forEach((repo) -> {
			executor.execute(new HttpRepositoryRequestThread(repo));
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
	 * Load many CKAN repositories concurrently
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Oct 1, 2016
	 */
	class HttpRepositoryRequestThread implements Runnable {

		String repository;

		CKANParserIMPL ckanParser;

		// CkanClient client;

		public HttpRepositoryRequestThread(String repository) {
			this.repository = repository;
			ckanParser = new CKANParserIMPL(repository, numberOfConcurrentRequests);
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
