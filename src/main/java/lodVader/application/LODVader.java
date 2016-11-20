/**
 * 
 */
package lodVader.application;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;

import fix.Fix;
import lodVader.application.fileparser.CKANRepositoryLoader;
import lodVader.application.fileparser.CkanToLODVaderConverter;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.loader.LODVaderConfigurator;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionDB.DistributionStatus;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CKANRepositoriesParser;
import lodVader.parsers.descriptionFileParser.Impl.CLODParser;
import lodVader.parsers.descriptionFileParser.Impl.DataIDParser;
import lodVader.parsers.descriptionFileParser.Impl.LODCloudParser;
import lodVader.parsers.descriptionFileParser.Impl.LOVParser;
import lodVader.parsers.descriptionFileParser.Impl.LinghubParser;
import lodVader.parsers.descriptionFileParser.Impl.RE3RepositoriesParser;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.plugins.intersection.subset.SubsetDetectionService;
import lodVader.plugins.intersection.subset.distribution.SubsetDistributionDetectionService;
import lodVader.plugins.intersection.subset.distribution.SubsetDetectorBFIntersectImpl;
import lodVader.streaming.LODVStreamFileImpl;
import lodVader.streaming.LODVStreamInterface;
import lodVader.streaming.LODVStreamInternetImpl;
import lodVader.tupleManager.processors.BasicStatisticalDataProcessor;
import lodVader.tupleManager.processors.BloomFilterProcessor2;
import lodVader.tupleManager.processors.SaveRawDataProcessor;

/**
 * @author Ciro Baron Neto
 * 
 *         Main LODVader Application
 * 
 *         Oct 1, 2016
 */
public class LODVader {

	final static Logger logger = LoggerFactory.getLogger(LODVader.class);

	static AtomicInteger distributionsBeingProcessed = new AtomicInteger(0);

	/**
	 * How many operation to run in parallel.
	 */
	int numberOfThreads = 4;
	
	/**
	 * Count unique triples
	 */
	boolean uniqPerDatasource = true;

	/**
	 * Streaming and processing
	 */
	boolean streamDistribution = false;
	boolean streamFromInternet = false;
	boolean createDumpOnDisk = false;
	boolean processStatisticalData = false;
	boolean createBloomFilter = false;

	/**
	 * Parsing options
	 */
	boolean parseLOV = false;
	boolean parseDBpedia = false;
	boolean parseLaundromat = false;
	boolean parseLODCloud = false;
	boolean parseRE3 = false;
	boolean parseCKANRepositories = false;
	boolean parseLinghub = false;

	/**
	 * BF options.
	 */
	// check if there already is a BF created for the distribution
	boolean ignoreCreatedBF = true;

	/**
	 * Main method
	 */
	public void Manager() {

		
		
		/**
		 * Load properties file, create MondoDB indexes, etc
		 */
		LODVaderConfigurator s = new LODVaderConfigurator();
		s.configure();

		if(uniqPerDatasource)
			countUniqPerDatasource();
		/**
		 * Start parsing files
		 */
		parseFiles();

		/**
		 * Stream and process distributions
		 */
		if (streamDistribution)
//			streamDistributions(DistributionDB.DistributionStatus.DONE);
		streamDistributions(DistributionDB.DistributionStatus.WAITING_TO_STREAM);

		// detectDatasets();

		logger.info("LODVader is done with the initial tasks. The API is running.");

	}
	
	public void countUniqPerDatasource(){
//		new DatasourcesUniqTriples(new LOVParser()).count();
//		new DatasourcesUniqTriples(new RE3RepositoriesParser(null, 0)).count();
		new DatasourcesUniqTriples(new CKANRepositoriesParser()).count();
		new DatasourcesUniqTriples(new LinghubParser(null)).count();
		new DatasourcesUniqTriples(new DataIDParser(null)).count();
		new DatasourcesUniqTriples(new CLODParser(null, null)).count();
		new DatasourcesUniqTriples(new LODCloudParser()).count();

	}

	/**
	 * Parse description files such as DCAT, VoID, DataID, CKAN repositories,
	 * etc.
	 */
	public void parseFiles() {

		logger.info("Parsing files...");

		DescriptionFileParserLoader loader = new DescriptionFileParserLoader();

		/**
		 * Parsing DBpedia DataID file
		 */
		if (parseDBpedia) {
			loader.load(new DataIDParser("http://downloads.dbpedia.org/2015-10/2015-10_dataid_catalog.ttl"));
			loader.parse();
		}

		/**
		 * Parsing LODLaundromat
		 */
		if (parseLaundromat) {
			loader.load(new CLODParser("http://cirola2000.cloudapp.net/files/urls", "ttl"));
			loader.parse();
		}

		/**
		 * Parsing Linked Open Vocabularies (lov.okfn.org)
		 */
		if (parseLOV) {
			loader.load(new LOVParser());
			loader.parse();
		}

		/**
		 * Parsing lod-cloud (lod-cloud.net)
		 */
		if (parseLODCloud) {
			loader.load(new LODCloudParser());
			loader.parse();
		}

		/**
		 * Parsing Linghub (linghub.lider-project.eu)
		 */
		if (parseLinghub) {
			loader.load(new LinghubParser("http://cirola2000.cloudapp.net/files/linghub.nt.gz"));
			// loader.load(new
			// LinghubParser("http://localhost/dbpedia/linghub.nt.gz"));
			loader.parse();
		}

		/**
		 * Parsing CKAN repositories (ckan.org/instances/#)
		 */
		if (parseCKANRepositories) {
			String datasource = "CKAN_REPOSITORIES";
			CKANRepositoryLoader ckanLoader = new CKANRepositoryLoader();
			ckanLoader.loadAllRepositories(CKANRepositories.ckanRepositoryList, datasource);
			new CkanToLODVaderConverter().convert(datasource);
			logger.info("Ckan parsing done");
		}

		/**
		 * Parsing RE3 CKAN instances
		 */
		if (parseRE3) {
			String datasource = "RE3_REPOSITORIES";
			CKANRepositoryLoader ckanLoader = new CKANRepositoryLoader();
			ckanLoader.loadAllRepositories(CKANRepositories.RE3Repositories, datasource);
			new CkanToLODVaderConverter().convert(datasource);
			logger.info("RE3 parsing done");
		}

	}

	public void streamDistributions(DistributionDB.DistributionStatus status) {
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		// load datasets with the status == waiting to stream
		GeneralQueriesHelper queries = new GeneralQueriesHelper();

		// load distributions to be analyzed
		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
				status.toString());

		logger.info("Streaming " + distributionsBeingProcessed.get() + " distributions with " + numberOfThreads
				+ " threads.");

		// for each object create a instance of distributionDB
		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);

			// if we are dealing wth bloom filter creation
			if (createBloomFilter) {

				// ignore distributions if BF has already been created
				if (ignoreCreatedBF) {
					GridFS gfsFile = new GridFS(DBSuperClass.getDBInstance(),
							BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES.toString());
					if (gfsFile.find(new BasicDBObject(BucketDB.DISTRIBUTION_ID, distribution.getID())).size() == 0) {
						executor.execute(new ProcessDataset(distribution));
						distributionsBeingProcessed.set(distributionsBeingProcessed.incrementAndGet());
					}

				}
			} else {
				executor.execute(new ProcessDataset(distribution));
				distributionsBeingProcessed.set(distributionsBeingProcessed.incrementAndGet());
			}
		}

		try {
			executor.awaitTermination(1234, TimeUnit.DAYS);
			executor.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		logger.info("And we are done processing everything!");
	}

	public void detectDatasets() {

		GeneralQueriesHelper queries = new GeneralQueriesHelper();

		distributionsBeingProcessed.set(0);

		BasicDBList andList = new BasicDBList();
		andList.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, false));
		andList.add(new BasicDBObject(DistributionDB.STATUS, DistributionDB.DistributionStatus.DONE.toString()));

		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject("$and", andList), null, DistributionDB.URI, 1);

		distributionsBeingProcessed.set(distributionObjects.size());

		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);
			executor.execute(new DetectSubsets(distribution));
		}

		executor.shutdown();
		try {
			executor.awaitTermination(20, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.info("And we are done discovering subsets!");
	}

	class DetectSubsets implements Runnable {

		DistributionDB distribution;

		/**
		 * Constructor for Class LODVader.DetectSubsets
		 */
		public DetectSubsets(DistributionDB distribution) {
			this.distribution = distribution;
		}

		@Override
		public void run() {
			logger.info("Discovering subset for " + distribution.getTitle() + "(" + distribution.getID() + "). "
					+ distributionsBeingProcessed.getAndDecrement() + " to go.");

			LODVaderIntersectionPlugin subsetDetector = new SubsetDetectorBFIntersectImpl();
			SubsetDetectionService subsetService = new SubsetDistributionDetectionService(subsetDetector, distribution);
			subsetService.saveSubsets();
		}
	}

	/**
	 * Class used to process a single dataset
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Oct 7, 2016
	 */
	class ProcessDataset implements Runnable {

		DistributionDB distribution;

		/**
		 * Constructor for Class LODVader.ProcessDataset
		 */
		public ProcessDataset(DistributionDB distribution) {
			this.distribution = distribution;
		}

		public void run() {

			Logger logger = LoggerFactory.getLogger(ProcessDataset.class);

			/**
			 * Check whether LODVader should stream from the internet of loca
			 * files
			 */
			LODVStreamInterface coreStream = null;

			if (streamFromInternet)
				coreStream = new LODVStreamInternetImpl();
			else
				coreStream = new LODVStreamFileImpl(LODVaderProperties.BASE_PATH + "/raw_files/");

			/**
			 * Registering statistical data processor
			 */
			BasicStatisticalDataProcessor basicStatisticalProcessor = null;
			if (processStatisticalData) {
				basicStatisticalProcessor = new BasicStatisticalDataProcessor(distribution);
				coreStream.getPipelineProcessor().registerProcessor(basicStatisticalProcessor);
			}

			/**
			 * Registering raw data processor
			 */
			SaveRawDataProcessor rawDataProcessor = null;
			if (createDumpOnDisk) {
				rawDataProcessor = new SaveRawDataProcessor(distribution, distribution.getID());
				coreStream.getPipelineProcessor().registerProcessor(rawDataProcessor);
			}

			/**
			 * Registering bloom filter processor
			 */
			BloomFilterProcessor2 bfProcessor = null;
			if (createBloomFilter) {
				bfProcessor = new BloomFilterProcessor2(distribution);
				coreStream.getPipelineProcessor().registerProcessor(bfProcessor);
			}

			// start processing
			try {
				distribution.setStatus(DistributionStatus.STREAMING);
				distribution.update();

				coreStream.startParsing(distribution);

				// after finishing processing, finalize the processors (save
				// data, etc etc).
				if (processStatisticalData)
					basicStatisticalProcessor.saveStatisticalData();

				if (createDumpOnDisk)
					rawDataProcessor.closeFile();

				if (createBloomFilter)
					bfProcessor.saveFilters();

				distribution.setStatus(DistributionStatus.DONE);

			} catch (Exception e) {

				// case get an exception, finalize the processors (save
				// data, etc etc).
				if (createDumpOnDisk)
					rawDataProcessor.closeFile();

				if (processStatisticalData)
					basicStatisticalProcessor.saveStatisticalData();

				if (createBloomFilter)
					bfProcessor.saveFilters();

				distribution.setLastMsg(e.getMessage());
				distribution.setStatus(DistributionStatus.ERROR);
				logger.error("ERROR! Distribution: " + distribution.getDownloadUrl() + " has status "
						+ distribution.getStatus().toString() + " with error msg '" + distribution.getLastMsg() + "'.");
			}

			try {
				distribution.update();
			} catch (LODVaderMissingPropertiesException e) {
				e.printStackTrace();

			}

			logger.info("Datasets to be processed: " + distributionsBeingProcessed.decrementAndGet());

		}
	}

}
