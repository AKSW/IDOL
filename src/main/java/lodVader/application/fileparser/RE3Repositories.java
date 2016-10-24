/**
 * 
 */
package lodVader.application.fileparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CKANParser;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class RE3Repositories {

	final static Logger logger = LoggerFactory.getLogger(RE3Repositories.class);

	// number of concurrent request to be made for each repository
	final int numberOfConcurrentRequests = 5;

	// number of repositories to be analyzed concurrently
	final int numberOfConcurrentRepositories = 6;
	
	static ArrayList<String> ckanRepositories = new ArrayList<>(Arrays.asList("http://datahub.io/",
			"http://catalog.data.gov/", "http://healthdata.gov/", "http://open.canada.ca/data/en/",
			"http://data.gov.au/", "http://data.bris.ac.uk/data/", "http://data.london.gov.uk/",
			"http://b2find.eudat.eu/", "https://www.geoplatform.gov/", "https://datastore.landcareresearch.co.nz/",
			"http://data.nhm.ac.uk", "https://wci.earth2observe.eu/data/", "https://open-data.europa.eu/en/data",
			"https://www.facs.org/quality-programs/cancer/ncdb", "https://repod.pon.edu.pl/", "http://mlvis.com/",
			"http://en.openei.org/datasets/", "http://openresearchdata.ch/", "http://search.geothermaldata.org/",
			"http://iatiregistry.org/"));

	public void loadAllRepositories() {

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

		CKANParser ckanParser;

		// CkanClient client;

		public HttpRepositoryRequestThread(String repository) {
			this.repository = repository;
			ckanParser = new CKANParser(repository, numberOfConcurrentRequests);
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
