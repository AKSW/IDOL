/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.ckanparser.CkanParser;
import lodVader.parsers.ckanparser.models.CkanDataset;
import lodVader.parsers.ckanparser.models.CkanResource;
import lodVader.parsers.descriptionFileParser.MetadataParser;
import lodVader.utils.FormatsUtils;

/**
 * CKAN parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public abstract class CKANParserIMPL extends MetadataParser {

	final static Logger logger = LoggerFactory.getLogger(CKANParserIMPL.class);
	
	String repositoryAddress;

	int numberOfConcurrentRequests;

	/**
	 * Constructor for Class CKANHelper
	 */
	public CKANParserIMPL(String parserName, String repositoryAddress, int numberOfConcurrentRequests) {
		super(parserName);
		this.repositoryAddress = repositoryAddress;
		this.numberOfConcurrentRequests = numberOfConcurrentRequests;
	}

	private CKANParserIMPL() {
		super(null);
	}

	/**
	 * Main methods for iterating over the datasets and saving them
	 * 
	 * @param datasets
	 *            list of CKAN datasets name
	 */
	public void saveInstances(List<String> datasets, String repository) {

		// set how many threads will run concurrently
		ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentRequests);

		// start the threads
		for (String dataset : datasets) {
			HTTPDatasetRequestThread worker = new HTTPDatasetRequestThread(dataset, repository);
			executor.execute(worker);
		}
		executor.shutdown();

		// wait for all threads to be done
		while (!executor.isTerminated()) {
		}

		logger.info("Finished all threads for " + repository);
	}

	/**
	 * Save a CkanDataset instance the main collection
	 * 
	 * @param the
	 *            CkanDataset
	 * @return the DatasetDB instance
	 */
	public DatasetDB saveDataset(CkanDataset dataset, String provenance) {

		return addDataset(dataset.getId(), false, dataset.getTitle(), dataset.getTitle(), provenance);
	}

	/**
	 * Save a CkanResource instance the main collection (a distribution)
	 * 
	 * @param the
	 *            CkanResource
	 * @return the DistributionDB instance
	 */
	public DistributionDB saveDistribution(CkanResource resource, DatasetDB datasetDB) {

		return addDistribution(resource.getUrl(), false, resource.getTitle(), resource.getFormat(), resource.getUrl(),
				datasetDB.getID(), datasetDB.getTitle(), getParserName(), repositoryAddress);

	}

	/**
	 * Class which will make the dataset requests for the CKAN repositories
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Sep 29, 2016
	 */
	class HTTPDatasetRequestThread implements Runnable {

		String dataset;

		String repository;

		/**
		 * Constructor for Class CKANHelper.WorkerThread
		 */
		public HTTPDatasetRequestThread(String ckanDataset, String repository) {
			this.dataset = ckanDataset;
			this.repository = repository;
		}

		@Override
		public void run() {
			CkanParser client = new CkanParser(repository);
			CkanDataset d = client.fetchDataset(dataset);
			FormatsUtils formatUtils = new FormatsUtils();
			DatasetDB datasetDB = null;

			for (CkanResource r : d.getResources()) {

				// only save the resources (distributions) which have some sort
				// of RDF serialization format
				if (!formatUtils.getJenaFormat(r.getFormat()).equals("")) {

					// only add a dataset if there is at least one RDF resource
					if (datasetDB == null)
						datasetDB = saveDataset(d, repository);
					DistributionDB distributionDB = saveDistribution(r, datasetDB);
					datasetDB.addDistributionID(distributionDB.getID());
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {
		CkanParser client;
		client = new CkanParser(repositoryAddress);
		List<String> datasets = client.fetchDatasetIds();
		logger.info("Loading datasets from repository: " + repositoryAddress);
		saveInstances(datasets, repositoryAddress);

	}
	

}
