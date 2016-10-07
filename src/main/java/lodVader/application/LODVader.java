/**
 * 
 */
package lodVader.application;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mongodb.DBObject;

import lodVader.enumerators.DistributionStatus;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.loader.LODVaderConfigurator;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.GeneralQueries;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CLODFileParser;
import lodVader.parsers.descriptionFileParser.Impl.DataIDFileParser;
import lodVader.parsers.descriptionFileParser.Impl.LOVParser;
import lodVader.parsers.descriptionFileParser.Impl.LodCloudParser;
import lodVader.streaming.LodVaderCoreStream;
import lodVader.tupleManager.PipelineProcessor;
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

	/**
	 * Main method
	 */
	public void Manager() {

		//
		LODVaderConfigurator s = new LODVaderConfigurator();
		s.configure();
		// parseFiles();

		ExecutorService executor = Executors.newFixedThreadPool(3);

		// load datasets with the status == waiting to stream
		GeneralQueries queries = new GeneralQueries();
		List<DBObject> distributionObjects = queries.getMongoDBObject(DistributionDB.COLLECTION_NAME,
				DistributionDB.STATUS, DistributionStatus.WAITING_TO_STREAM.toString());

		// for each object create a instance of distributionDB
		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);
			executor.execute(new ProcessDataset(distribution));
		}

		try {
			executor.awaitTermination(1234, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		executor.shutdown();

	}

	/**
	 * Parse description files such as DCAT, VoID, DataID, CKAN repositories,
	 * etc.
	 */
	public void parseFiles() {

		// load ckan repositories into lodvader
		// CKANRepositories ckanParsers = new CKANRepositories();
		// ckanParsers.loadAllRepositories();

		DescriptionFileParserLoader loader = new DescriptionFileParserLoader();
		loader.load(new LodCloudParser());
		loader.parse();
		loader.load(new LOVParser());
		loader.parse();
		loader.load(new CLODFileParser("http://cirola2000.cloudapp.net/files/urls", "ttl"));
		// loader.load(new CLODFileParser("http://localhost/urls", "ttl"));
		loader.parse();
		loader.load(new DataIDFileParser("http://downloads.dbpedia.org/2015-10/2015-10_dataid_catalog.ttl"));
		loader.parse();

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

			// load the main LODVader streamer
			LodVaderCoreStream coreStream = new LodVaderCoreStream();

			// create a pipeline processor and register it on the streamer
			PipelineProcessor pipelineProcessor = new PipelineProcessor();
			coreStream.registerPipelineProcessor(pipelineProcessor);

			// create some processors
			BasicStatisticalDataProcessor basicStatisticalProcessor = new BasicStatisticalDataProcessor(distribution);
			BloomFilterProcessor bfProcessor = new BloomFilterProcessor(distribution);

			// register them into the pipeline
			pipelineProcessor.registerProcessor(basicStatisticalProcessor);
			pipelineProcessor.registerProcessor(bfProcessor);

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

		}
	}

}
