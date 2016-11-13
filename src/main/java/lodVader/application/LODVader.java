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

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.loader.LODVaderConfigurator;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionDB.DistributionStatus;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.LOVParser;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.plugins.intersection.subset.SubsetDetectionService;
import lodVader.plugins.intersection.subset.distribution.SubsetDistributionDetectionService;
import lodVader.plugins.intersection.subset.distribution.SubsetDistributionDetectorBFImpl;
import lodVader.streaming.LODVaderCoreStream;
import lodVader.streaming.LODVaderRawDataStream;
import lodVader.tupleManager.processors.SaveRawDataProcessor;

/**
 * @author Ciro Baron Neto
 * 
 *         Main LODVader Application
 * 
 *         Oct 1, 2016
 */
public class LODVader {

	// public static void main(String[] args) {
	// new LODVader().Manager();
	// }

	final static Logger logger = LoggerFactory.getLogger(LODVader.class);

	static AtomicInteger distributionsBeingProcessed = new AtomicInteger(0);

	int numberOfThreads = 6;

	/**
	 * Main method
	 */
	public void Manager() {

		// new Fix().fix3();

		LODVaderConfigurator s = new LODVaderConfigurator();
		s.configure();
		// //
//		 parseFiles();
		streamDistributions();
		// detectDatasets();

		logger.info("LODVader is done with the initial tasks. The API is running.");

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
		// loader.load(new
		// DataIDFileParser("http://downloads.dbpedia.org/2015-10/2015-10_dataid_catalog.ttl"));
		// loader.parse();

		/**
		 * Parsing LODLaundromat
		 */
		// loader.load(new
		// CLODFileParser("http://cirola2000.cloudapp.net/files/urls", "ttl"));
		// loader.parse();

		/**
		 * Parsing Linked Open Vocabularies (lov.okfn.org)
		 */
		 loader.load(new LOVParser());
		 loader.parse();

		/**
		 * Parsing lod-cloud (lod-cloud.net)
		 */
		// loader.load(new LODCloudParser());
		// loader.parse();

		/**
		 * Parsing Linghub (linghub.lider-project.eu)
		 */
		// loader.load(new
		// LinghubParser("http://cirola2000.cloudapp.net/files/linghub.nt.gz"));
		// loader.load(new
		// LinghubParser("http://localhost/dbpedia/linghub.nt.gz"));
		// loader.parse();

		/**
		 * Parsing CKAN repositories (ckan.org/instances/#)
		 */
//		String datasource = "CKAN_REPOSITORIES";
		// CKANRepositoryLoader ckanLoader = new CKANRepositoryLoader();
		// ckanLoader.loadAllRepositories(CKANRepositories.ckanRepositoryList,
		// datasource);
//		new CkanToLODVaderConverter().convert(datasource);
		//
		// logger.info("Ckan parsing done");

		/**
		 * Parsing RE3 CKAN instances
		 */
//		datasource = "RE3_REPOSITORIES";
		// CKANRepositoryLoader ckanLoader = new CKANRepositoryLoader();
		// ckanLoader.loadAllRepositories(CKANRepositories.RE3Repositories,
		// datasource);
//		new CkanToLODVaderConverter().convert(datasource);
//		logger.info("RE3 parsing done");


	}

	public void streamDistributions() {
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		// load datasets with the status == waiting to stream
		GeneralQueriesHelper queries = new GeneralQueriesHelper();


//		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.DOWNLOAD_URL,
//		"http://lov.okfn.org/dataset/lov/sparql?query=CONSTRUCT+%7B+%3Fs+%3Fp+%3Fo+%7D+WHERE+%7B+GRAPH+%3Chttp%3A%2F%2Fwww.w3.org%2F2005%2FIncubator%2Fssn%2Fssnx%2Fssn%3E+%7B+%3Fs+%3Fp+%3Fo+%7D+%7D");
		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, new BasicDBObject());
//		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
//		DistributionDB.DistributionStatus.WAITING_TO_STREAM.toString());
//		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
//				DistributionDB.DistributionStatus.DONE.toString());

		distributionsBeingProcessed.set(distributionObjects.size());

		logger.info("Discovering subset for " + distributionsBeingProcessed.get() + " distributions with "
				+ numberOfThreads + " threads.");
		// for each object create a instance of distributionDB
		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);
			executor.execute(new ProcessDataset(distribution));
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

		System.err.println(new BasicDBObject("$and", andList));

		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject("$and", andList), null, DistributionDB.URI, 1);

		distributionsBeingProcessed.set(distributionObjects.size());

		ExecutorService executor = Executors.newFixedThreadPool(6);

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

			LODVaderIntersectionPlugin subsetDetector = new SubsetDistributionDetectorBFImpl();
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
			if (!distribution.getStatus().equals(DistributionDB.DistributionStatus.ERROR)) {
				Logger logger = LoggerFactory.getLogger(ProcessDataset.class);

				// load the main LODVader streamer
				LODVaderCoreStream coreStream = new LODVaderCoreStream();
//				LODVaderRawDataStream coreStream = new LODVaderRawDataStream(LODVaderProperties.BASE_PATH + "/raw_files/");

				// create some processors
//				BasicStatisticalDataProcessor basicStatisticalProcessor = new BasicStatisticalDataProcessor(
//						distribution);
				SaveRawDataProcessor rawDataProcessor = new SaveRawDataProcessor(distribution, distribution.getID());
//				BloomFilterProcessor2 bfProcessor = new BloomFilterProcessor2(distribution);

				// register them into the pipeline
//				 coreStream.getPipelineProcessor().registerProcessor(basicStatisticalProcessor);
				 coreStream.getPipelineProcessor().registerProcessor(rawDataProcessor);
//				coreStream.getPipelineProcessor().registerProcessor(bfProcessor);

				// start processing
				try {
					distribution.setStatus(DistributionStatus.STREAMING);
					distribution.update();

					coreStream.startParsing(distribution);
					// after finishing processing, finalize the processors (save
					// data, etc etc).
//					 basicStatisticalProcessor.saveStatisticalData();
					
					rawDataProcessor.closeFile();
//					bfProcessor.saveFilters();
					distribution.setStatus(DistributionStatus.DONE);
				} catch (Exception e) {
					rawDataProcessor.closeFile();
//					bfProcessor.saveFilters();
//					basicStatisticalProcessor.saveStatisticalData();

					distribution.setLastMsg(e.getMessage());
					distribution.setStatus(DistributionStatus.ERROR);
					logger.error("ERROR! Distribution: " + distribution.getDownloadUrl() + " has status "
							+ distribution.getStatus().toString() + " with error msg '" + distribution.getLastMsg()
							+ "'.");
					// e.printStackTrace();
				}

				try {
					distribution.update();
				} catch (LODVaderMissingPropertiesException e) {
					e.printStackTrace();
				}
			}

			logger.info("Datasets to be processed: " + distributionsBeingProcessed.decrementAndGet());

		}
	}

}
