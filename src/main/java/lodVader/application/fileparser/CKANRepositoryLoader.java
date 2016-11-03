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

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.ckanparser.CkanCatalogDB;
import lodVader.mongodb.collections.ckanparser.CkanDatasetDB;
import lodVader.mongodb.collections.ckanparser.adapters.CkanCatalogDBAdapter;
import lodVader.mongodb.collections.ckanparser.adapters.CkanDatasetDBAdapter;
import lodVader.parsers.ckanparser.CkanDatasetList;
import lodVader.parsers.ckanparser.CkanParser;
import lodVader.parsers.ckanparser.models.CkanDataset;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class CKANRepositoryLoader {

	final static Logger logger = LoggerFactory.getLogger(CKANRepositoryLoader.class);

	// number of concurrent request to be made for each repository
	final int numberOfConcurrentRequests = 10;

	// number of repositories to be analyzed concurrently
	final int numberOfConcurrentRepositories = 10;
	

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
			int i = 0;
			while(list.hasNext()){
				if(i++==30)
					break;
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