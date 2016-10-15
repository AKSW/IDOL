/**
 * 
 */
package lodVader.application;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;

import fix.Fix;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionDB.DistributionStatus;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.LOVParser;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.plugins.intersection.subset.linkset.LinksetDetectionService;
import lodVader.plugins.intersection.subset.linkset.LinksetDetectorBFImpl;
import lodVader.streaming.LODVaderCoreStream;
import lodVader.tupleManager.processors.BasicStatisticalDataProcessor;
import lodVader.tupleManager.processors.BloomFilterProcessor;

/**
 * @author Ciro Baron Neto
 * 
 *         Main LODVader Application
 * 
 *         Oct 1, 2016
 */
public class LODVader {

//	public static void main(String[] args) {
//		new LODVader().Manager();
//	}

	final static Logger logger = LoggerFactory.getLogger(LODVader.class);

	AtomicInteger distributionsBeingProcessed = new AtomicInteger(0);

	int numberOfThreads = 6;

	/**
	 * Main method
	 */
	public void Manager() {
		
		new Fix();

//		LODVaderConfigurator s = new LODVaderConfigurator();
//		s.configure();

//		 parseFiles();
//		 streamDistributions();
//		detectDatasets();

	}

	/**
	 * Parse description files such as DCAT, VoID, DataID, CKAN repositories,
	 * etc.
	 */
	public void parseFiles() {

		logger.info("Parsing files...");
		// load ckan repositories into lodvader
//		 CKANRepositories ckanParsers = new CKANRepositories();
		// ckanParsers.loadAllRepositories();

		DescriptionFileParserLoader loader = new DescriptionFileParserLoader();
		// loader.load(new
		// CLODFileParser("http://cirola2000.cloudapp.net/files/urls", "ttl"));
		// loader.load(new CLODFileParser("http://localhost/urls", "ttl"));
		loader.load(new LOVParser());
		loader.parse();
		// loader.load(new
		// DataIDFileParser("http://downloads.dbpedia.org/2015-10/2015-10_dataid_catalog.ttl"));
		// loader.parse();
		// loader.load(new
		// CLODFileParser("http://cirola2000.cloudapp.net/files/urls", "ttl"));
		// loader.parse();
		// loader.load(new LodCloudParser());
		// loader.parse();

	}

	public void streamDistributions() {
		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
		// load datasets with the status == waiting to stream
		GeneralQueriesHelper queries = new GeneralQueriesHelper();
		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
				DistributionStatus.WAITING_TO_STREAM.toString());

		distributionsBeingProcessed.set(distributionObjects.size());

		logger.info("Discovering subset for " + distributionsBeingProcessed.get() + " distributions with "
				+ numberOfThreads + " threads.");
		// for each object create a instance of distributionDB
		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);
			// DistributionDB distribution = new DistributionDB();
			// distribution.find(true, DistributionDB.ID,
			// "57f786ddb5c0f614b88dbae9");
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

		List<DBObject> distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
				DistributionStatus.WAITING_TO_STREAM.toString());

		distributionsBeingProcessed.set(distributionObjects.size());

		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);
			logger.info("Discovering subset for " + distribution.getTitle() + "("+ distribution.getID()+"). "
					+ distributionsBeingProcessed.getAndDecrement() + " to go.");

//			LODVaderIntersectionPlugin subsetDetector = new SubsetDistributionDetectorBFImpl();
//			LODVaderIntersectionPlugin subsetDetector = new SubsetDetectorHashSetImpl();
//			SubsetDetectionService subsetService = new SubsetDistributionDetectionService(subsetDetector, distribution); 
//			subsetService.saveSubsets();
			LODVaderIntersectionPlugin linksetDetector = new LinksetDetectorBFImpl();
			LinksetDetectionService linksetService = new LinksetDetectionService(linksetDetector, distribution); 
			linksetService.saveSubsets();

		}

		logger.info("And we are done discovering subsets!");
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

			// load the main LODVader streamer
			LODVaderCoreStream coreStream = new LODVaderCoreStream();

			// create some processors
			BasicStatisticalDataProcessor basicStatisticalProcessor = new BasicStatisticalDataProcessor(distribution);
			BloomFilterProcessor bfProcessor = new BloomFilterProcessor(distribution);

			// register them into the pipeline
			coreStream.getPipelineProcessor().registerProcessor(basicStatisticalProcessor);
			coreStream.getPipelineProcessor().registerProcessor(bfProcessor);

			// start processing
			try {
				distribution.setStatus(DistributionStatus.STREAMING);
				distribution.update();

				coreStream.startParsing(distribution);
				// after finishing processing, finalize the processors (save
				// data, etc etc).
				basicStatisticalProcessor.saveStatisticalData();
				bfProcessor.saveFilters();
				distribution.setStatus(DistributionStatus.DONE);
			} catch (IOException | LODVaderLODGeneralException | LODVaderFormatNotAcceptedException
					| LODVaderMissingPropertiesException e) {
				distribution.setLastMsg(e.getMessage());
				distribution.setStatus(DistributionStatus.ERROR);
				e.printStackTrace();
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
