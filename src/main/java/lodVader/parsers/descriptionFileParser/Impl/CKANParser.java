/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Impl;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.trentorise.opendata.jackan.ckan.CkanClient;
import eu.trentorise.opendata.jackan.ckan.CkanDataset;
import eu.trentorise.opendata.jackan.ckan.CkanResource;
import lodVader.enumerators.DistributionStatus;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.interfaces.DescriptionFileParserInterface;
import lodVader.utils.FormatsUtils;

/**
 * CKAN parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class CKANParser implements DescriptionFileParserInterface {

	final static Logger logger = LoggerFactory.getLogger(CKANParser.class);

	HashMap<String, DistributionDB> distributions = new HashMap<String, DistributionDB>();

	HashMap<String, DatasetDB> datasets = new HashMap<String, DatasetDB>();

	String repositoryAddress = null;

	int numberOfConcurrentRequests;


	/**
	 * Constructor for Class CKANHelper
	 */
	public CKANParser(String repositoryAddress, int numberOfConcurrentRequests) {
		this.repositoryAddress = repositoryAddress;
		this.numberOfConcurrentRequests = numberOfConcurrentRequests;
	}

	private CKANParser() {
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

		DatasetDB datasetDB = new DatasetDB();
		datasetDB.setIsVocabulary(false);
		datasetDB.setTitle(dataset.getTitle());
		datasetDB.setLabel(dataset.getTitle());
		datasetDB.setDescriptionFileParser(getParserName());
		datasetDB.addProvenance(provenance);
		datasetDB.setUri(dataset.getName());
		logger.info("Dataset found: " + dataset.getName());

		datasets.put(datasetDB.getUri(), datasetDB);

		return datasetDB;
	}

	/**
	 * Save a CkanResource instance the main collection (a distribution)
	 * 
	 * @param the
	 *            CkanResource
	 * @return the DistributionDB instance
	 */
	public DistributionDB saveDistribution(CkanResource resource, DatasetDB datasetDB) {

		FormatsUtils formatsUtils = new FormatsUtils();
		
		DistributionDB distributionDB = new DistributionDB();
		distributionDB.setTitle(resource.getName());

		try {
			distributionDB.setDownloadUrl(resource.getUrl());
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		distributionDB.setFormat(FormatsUtils.getEquivalentFormat(resource.getFormat()));
		distributionDB.setUri(resource.getUrl());
		distributionDB.setTopDataset(datasetDB.getID()); 
		distributionDB.setTopDatasetTitle(datasetDB.getTitle());
		distributionDB.setStatus(DistributionStatus.WAITING_TO_STREAM);
		distributions.put(distributionDB.getUri(), distributionDB);

		return distributionDB;

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
			CkanClient client = new CkanClient(repository);
			CkanDataset d = client.getDataset(dataset);
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
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#
	 * getDistributions()
	 */
	@Override
	public List<DistributionDB> getDistributions() {
		return new ArrayList<DistributionDB>(distributions.values());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getDatasets()
	 */
	@Override
	public List<DatasetDB> getDatasets() {
		return new ArrayList<DatasetDB>(datasets.values());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {

		CkanClient client;
		client = new CkanClient(repositoryAddress);
		List<String> datasets = client.getDatasetList();
		logger.info("Loading datasets from repository: " + repositoryAddress);
		saveInstances(datasets, repositoryAddress);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getParserName(
	 * )
	 */
	@Override
	public String getParserName() {
		return "CKAN_PARSER";
	}
	
	/* (non-Javadoc)
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#getRepositoryAddress()
	 */
	@Override
	public String getRepositoryAddress() {
		return repositoryAddress;
	}

}
