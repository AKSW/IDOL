/**
 * 
 */
package org.aksw.idol.application.fileparser;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.aksw.idol.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.mongodb.collections.ckanparser.CkanCatalogDB;
import org.aksw.idol.mongodb.collections.ckanparser.CkanDatasetDB;
import org.aksw.idol.mongodb.collections.ckanparser.adapters.CkanCatalogDBAdapter;
import org.aksw.idol.mongodb.collections.ckanparser.adapters.CkanDatasetDBAdapter;
import org.aksw.idol.parsers.ckanparser.CkanDatasetList;
import org.aksw.idol.parsers.ckanparser.CkanParser;
import org.aksw.idol.parsers.ckanparser.models.CkanDataset;
import org.aksw.idol.parsers.descriptionFileParser.Impl.CKANParserIMPL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class CKANRepositoryLoader {
	

	final static Logger logger = LoggerFactory.getLogger(CKANRepositoryLoader.class);

	// number of concurrent request to be made for each repository
	final int numberOfConcurrentRequests = 5;

	// number of repositories to be analyzed concurrently
	final int numberOfConcurrentRepositories = 20;
	

	public void loadAllRepositories(List<String> ckanRepositories, String datasource) {

		ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentRepositories);
	
		ckanRepositories.forEach((repo) -> {
			executor.execute(new HttpRepositoryRequestThread(repo,datasource));
		});

		executor.shutdown();
		try {
			executor.awaitTermination(300, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("All CKAN repositories loaded.");

	}

	/**
	 * Load many CKAN repositories concurrently
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Oct 1, 2016
	 */
	class HttpRepositoryRequestThread implements Runnable {

		String ckanCatalog;
		String datasource;


		// CkanClient client;

		public HttpRepositoryRequestThread(String ckanCatalog, String datasource) {
			this.ckanCatalog = ckanCatalog;
			this.datasource= datasource;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			CkanParser parser = new CkanParser(ckanCatalog);
			CkanDatasetList list = parser.getDatasetList();
			
			CkanCatalogDB catalogDB = new CkanCatalogDBAdapter(parser.getCkanCatalog());
	
			try {
				catalogDB.update();
			} catch (Exception e) { 
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			while(list.hasNext()){
				CkanDataset dataset = list.next();
				CkanDatasetDB datasetDB = new CkanDatasetDBAdapter(dataset, ckanCatalog, datasource);
				
				try {
					datasetDB.update();
				} catch (LODVaderMissingPropertiesException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}
	}

}