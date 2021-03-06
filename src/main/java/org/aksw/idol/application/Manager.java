
/**
 * 
 */
package org.aksw.idol.application;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aksw.idol.application.fileparser.CKANRepositoryLoader;
import org.aksw.idol.application.fileparser.CkanToLODVaderConverter;
import org.aksw.idol.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.loader.LODVaderConfigurator;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.mongodb.DBSuperClass;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.DistributionDB.DistributionStatus;
import org.aksw.idol.mongodb.collections.LinkIndegree;
import org.aksw.idol.mongodb.collections.LinkOutdegree;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceRelationDB;
import org.aksw.idol.mongodb.collections.datasetBF.BucketDB;
import org.aksw.idol.mongodb.queries.GeneralQueriesHelper;
import org.aksw.idol.parsers.descriptionFileParser.DescriptionFileParserLoader;
import org.aksw.idol.parsers.descriptionFileParser.Impl.CKANRepositoriesParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.CLODParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.DataIDParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.LODCloudParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.LOVParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.LinghubParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.LodStatsMainParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.RE3RepositoriesParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.SparqlesMainParser;
import org.aksw.idol.plugins.intersection.subset.linkset.LinksetDetectionHelper;
import org.aksw.idol.properties.DataSourcesProperties;
import org.aksw.idol.properties.IDOLProperties;
import org.aksw.idol.properties.ParseProperties;
import org.aksw.idol.properties.Properties;
import org.aksw.idol.streaming.IDOLFileStreamImpl;
import org.aksw.idol.streaming.IDOLStreamInterface;
import org.aksw.idol.streaming.IDOLStreamInternetImpl;
import org.aksw.idol.tupleManager.processors.BasicStatisticalDataProcessor;
import org.aksw.idol.tupleManager.processors.BloomFilterProcessor;
import org.aksw.idol.tupleManager.processors.SaveDumpDataProcessor;
import org.aksw.idol.uniq.DatasourcesUniqTriples;
import org.aksw.idol.uniq.DatasourcesUniqTriplesExternalSort;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;

/**
 * @author Ciro Baron Neto
 * 
 *         Main LODVader Application
 * 
 *         Oct 1, 2016
 */
@Async
public class Manager {

	final static Logger logger = LoggerFactory.getLogger(Manager.class);

	static AtomicInteger distributionsBeingProcessed = new AtomicInteger(0);

	@Autowired
	Properties properties;

	// @Autowired
	// DatasourcesUniqTriples datasourcesUniqTriples;

	@Autowired
	DatasourcesUniqTriplesExternalSort datasourcesUniqTriples;

	/**
	 * How many operation to run in parallel.
	 */
	int numberOfThreads;

	/**
	 * Streaming and processing
	 */
	boolean streamDistribution;
	boolean streamFromInternet;

	boolean createDumpOnDisk;
	boolean overrideDumpOnDisk;

	boolean processStatisticalData;
	boolean createBloomFilter;

	/**
	 * Parsing options
	 */
	boolean parseSparqles;
	boolean parseLOV;
	boolean parseDBpedia;
	boolean parseLaundromat;
	boolean parseLODCloud;
	boolean parseRE3;
	boolean parseCKANRepositories;
	boolean parseLinghub;
	boolean parseLodStats;

	/**
	 * BF options.
	 */
	// check if there already is a BF created for the distribution
	boolean ignoreCreatedBF;

	/**
	 * Detect overlapping datasets
	 */
	boolean detectOverlappingDatasets = false;

	/**
	 * Main method
	 */
	public void start() {
		setProperties();
		/**
		 * Load properties file, create MondoDB indexes, etc
		 */
		LODVaderConfigurator s = new LODVaderConfigurator();
		s.configure();

		// new MetadataParserServices().removeDistributions(new
		// DataIDParser(null));

		/**
		 * Start parsing files
		 */
		parseFiles();

		countUniqPerDatasource();

		/**
		 * Stream and process distributions
		 */
		if (streamDistribution)
			// streamDistributions(DistributionDB.DistributionStatus.ERROR);
			// streamDistributions(DistributionDB.DistributionStatus.WAITING_TO_STREAM);
			// streamDistributions(DistributionDB.DistributionStatus.DONE);
			streamDistributions(null);

		if (detectOverlappingDatasets)
			detectDatasets();

		// detectDBPediaDatasets();
		// addDatasetsIntoRelations(GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS0);
		// addDatasetsIntoRelations(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0);

		detectOutdegree();
		//
		detectIndegree();

		logger.info("LODVader is done with the initial tasks. The API is running.");

	}

	/**
	 * Count unique triples per datasource
	 */
	public void countUniqPerDatasource() {
		DataSourcesProperties datasources = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource()
				.getDataSources();

		if (datasources.isLodlaundromat())
			datasourcesUniqTriples.setup(new CLODParser(null, null));

		if (datasources.isLov())
			datasourcesUniqTriples.setup(new LOVParser(null));

		if (datasources.isRe3())
			datasourcesUniqTriples.setup(new RE3RepositoriesParser(null, 0));

		if (datasources.isLinghib())
			datasourcesUniqTriples.setup(new LinghubParser(null));

		if (datasources.isDbpedia())
			datasourcesUniqTriples.setup(new DataIDParser(null));

		if (datasources.isLodcloud())
			datasourcesUniqTriples.setup(new LODCloudParser(null));

		if (datasources.isCkanrepositories())
			datasourcesUniqTriples.setup(new CKANRepositoriesParser());

		if (datasources.isLodstats())
			datasourcesUniqTriples.setup(new LodStatsMainParser(null));

	}

	/**
	 * Parse description files such as DCAT, VoID, DataID, CKAN repositories, etc.
	 */
	public void parseFiles() {

		logger.info("Parsing files...");

		DescriptionFileParserLoader loader = new DescriptionFileParserLoader();

		ParseProperties parse = properties.getIdolproperties().getParse();

		/**
		 * Parsing DBpedia DataID file
		 */
		if (parseDBpedia) {
			loader.load(new DataIDParser(parse.getDbpedia().getUrl().toString()));
			loader.parse();
		}

		/**
		 * Parsing LODLaundromat
		 */
		if (parseLaundromat) {
			loader.load(new CLODParser(parse.getLodlaundromat().getUrl().toString(), "ttl"));
			loader.parse();
		}

		/**
		 * Parsing Sparqles
		 */
		if (parseSparqles) {
			loader.load(new SparqlesMainParser(parse.getSparqles().getUrl().toString()));
			loader.parse();
		}

		/**
		 * Parsing Linked Open Vocabularies (lov.okfn.org)
		 */
		if (parseLOV) {
			loader.load(new LOVParser(parse.getLov().getUrl().toString()));
			loader.parse();
		}

		/**
		 * Parsing LodStats
		 */
		if (parseLodStats) {
			loader.load(new LodStatsMainParser(parse.getLodstats().getUrl().toString()));
			loader.parse();
		}

		/**
		 * Parsing lod-cloud (lod-cloud.net)
		 */
		if (parseLODCloud) {
			loader.load(new LODCloudParser(parse.getLodcloud().getUrl().toString()));
			loader.parse();
		}

		/**
		 * Parsing Linghub (linghub.lider-project.eu)
		 */
		if (parseLinghub) {
			loader.load(new LinghubParser(parse.getLinghub().getUrl().toString()));
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
		List<DBObject> distributionObjects = null;
		if (status != null)
			distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS,
					status.toString());
		else
			distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, new BasicDBObject());

		// distributionObjects =
		// queries.getObjects(DistributionDB.COLLECTION_NAME,
		// new BasicDBObject(DistributionDB.DATASOURCE, new
		// LodStatsMainParser().getParserName()));

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

		distributionsBeingProcessed.set(0);

		BasicDBList andList = new BasicDBList();
		// andList.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, false));
		andList.add(new BasicDBObject(DistributionDB.STATUS, DistributionDB.DistributionStatus.DONE.toString()));

		// List<DBObject> distributionObjects =
		// queries.getObjects(DistributionDB.COLLECTION_NAME,
		// new BasicDBObject("$and", andList), null, DistributionDB.URI, 1);
		List<DBObject> distributionObjects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject(DistributionDB.STATUS, DistributionDB.DistributionStatus.DONE.toString()));

		distributionsBeingProcessed.set(distributionObjects.size());

		logger.info("Discovering subsets for " + distributionObjects.size() + " distributions.");

		ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);
			executor.execute(new SubsetDetector(distribution));
		}

		executor.shutdown();
		try {
			executor.awaitTermination(201, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.info("And we are done discovering subsets!");
	}

	public void detectOutdegree() {
		logger.info("Detecting outdegree!");

		List<DBObject> distributionObjects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		HashMap<String, Integer> h = new HashMap<>();
		ExecutorService ex = Executors.newFixedThreadPool(3);

		int minResources = 50;

		for (DBObject object : distributionObjects) {

			Runnable r = () -> {
				DistributionDB distribution = new DistributionDB(object);

				List<String> distids = new LinksetDetectionHelper().loadOutdegreeTargetDatasetsIds(distribution,
						minResources);

				h.put(distribution.getDownloadUrl(), distids.size());

				LinkOutdegree link = new LinkOutdegree();
				link.setdataset(distribution.getTopDatasetID());
				link.setAmount(distids.size());
				if (distids.size() > 0)
					try {
						link.update();
					} catch (LODVaderMissingPropertiesException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			};

			ex.submit(r);
		}
		ex.shutdown();
		try {
			ex.awaitTermination(123, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.info("Done detecting outdegree!");

	}

	public void detectIndegree() {
		logger.info("Detecting indegree!");

		List<DBObject> distributionObjects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());
		int minResources = 50;

		HashMap<String, Integer> h = new HashMap<>();
		ExecutorService ex = Executors.newFixedThreadPool(3);

		for (DBObject object : distributionObjects) {

			Runnable r = () -> {
				DistributionDB distribution = new DistributionDB(object);

				List<String> distids = new LinksetDetectionHelper().loadIndegreeTargetDatasetsIds(distribution,
						minResources);

				h.put(distribution.getDownloadUrl(), distids.size());

				LinkIndegree link = new LinkIndegree();
				link.setdataset(distribution.getTopDatasetID());
				link.setAmount(distids.size());
				if (distids.size() > 0)
					try {
						link.update();
					} catch (LODVaderMissingPropertiesException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			};

			ex.submit(r);
		}
		ex.shutdown();
		try {
			ex.awaitTermination(123, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.info("Done detection indegree!");

	}

	public void addDatasetsIntoRelations(GeneralResourceRelationDB.COLLECTIONS collection) {
		List<DBObject> distributionObjects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		HashMap<String, Integer> h = new HashMap<>();
		int i = 0;

		for (DBObject o : distributionObjects) {
			int j = 0;
			DistributionDB distribution = new DistributionDB(o);
			BasicDBObject query = new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distribution.getID());
			GeneralResourceRelationDB.getCollection(collection.toString()).find(query).forEach((object) -> {
				GeneralResourceRelationDB v = new GeneralResourceRelationDB(collection, object);
				v.setDatasetID(distribution.getTopDatasetID());
				// System.out.println(new ObjectID(v.getID()));
				// if (v.getDatasetID() == null)
				new GeneralResourceRelationDB(collection).getCollection(collection.toString())
						.update(new BasicDBObject("_id", new ObjectId(v.getID())), v.mongoDBObject, true, false);

				// System.out.println("l");
				// try {
				// v.update();
				// } catch (Exception e) {
				// // TODO Auto-generated catch block
				// System.out.println(v.getID());
				// e.printStackTrace();
				// }

			});

			System.out.println("updated " + distribution.getDownloadUrl());
			System.out.println("dataset nr: " + i++);

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
			 * Check whether LODVader should stream from the internet of loca files
			 */
			IDOLStreamInterface coreStream = null;

			if (streamFromInternet)
				coreStream = new IDOLStreamInternetImpl();
			else {
				coreStream = new IDOLFileStreamImpl(LODVaderProperties.BASE_PATH + "/raw_files/");
			}

			/**
			 * Registering statistical data processor
			 */
			BasicStatisticalDataProcessor basicStatisticalProcessor = null;
			if (processStatisticalData) {
				basicStatisticalProcessor = new BasicStatisticalDataProcessor(distribution);
				coreStream.getPipelineProcessor().registerProcessor(basicStatisticalProcessor);
			}

			/**
			 * Registering raw data processor if there already is a file named with the same
			 * id, do not stream again
			 */
			SaveDumpDataProcessor saveDumpDataProcessor = null;
			if (createDumpOnDisk) {
				if (overrideDumpOnDisk || !new File(LODVaderProperties.RAW_FILE_PATH + distribution.getID()).exists()) {
					saveDumpDataProcessor = new SaveDumpDataProcessor(distribution, distribution.getID());
					coreStream.getPipelineProcessor().registerProcessor(saveDumpDataProcessor);
				} else {
					logger.info("File: " + LODVaderProperties.RAW_FILE_PATH + distribution.getID()
							+ " already exists. We are not overriding it.");
				}
			}

			/**
			 * Registering bloom filter processor
			 */
			BloomFilterProcessor bfProcessor = null;
			if (createBloomFilter) {
				bfProcessor = new BloomFilterProcessor(distribution);
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
					if (saveDumpDataProcessor != null)
						saveDumpDataProcessor.closeFile();

				if (createBloomFilter)
					bfProcessor.saveFilters();

				distribution.setStatus(DistributionStatus.DONE);

			} catch (Exception e) {

				// e.printStackTrace();

				// case get an exception, finalize the processors (save
				// data, etc etc).
				if (createDumpOnDisk)
					if (saveDumpDataProcessor != null)
						saveDumpDataProcessor.closeFile();

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

	private void setProperties() {

		numberOfThreads = properties.getIdolproperties().getNrthreads();
		if (properties.getIdolproperties().getStreaming() != null) {
			streamDistribution = true;
			if (properties.getIdolproperties().getStreaming() == IDOLProperties.Streaming.INTERNET) {
				streamFromInternet = true;
			} else if (properties.getIdolproperties().getStreaming() == IDOLProperties.Streaming.LOCAL) {
				streamFromInternet = false;
			}

		} else {
			streamDistribution = false;
		}
		createDumpOnDisk = properties.getIdolproperties().getTasks().getCreateDumpOnDisk();
		overrideDumpOnDisk = properties.getIdolproperties().getTasks().getOverrideDumpOnDisk();
		processStatisticalData = properties.getIdolproperties().getTasks().isProcessstatisticaldata();
		createBloomFilter = properties.getIdolproperties().getTasks().getCreateDatasetsBloomFilter();

		parseSparqles = properties.getIdolproperties().getParse().getSparqles().isStream();
		parseLOV = properties.getIdolproperties().getParse().getLov().isStream();
		parseDBpedia = properties.getIdolproperties().getParse().getDbpedia().isStream();
		parseLaundromat = properties.getIdolproperties().getParse().getLodlaundromat().isStream();
		parseLODCloud = properties.getIdolproperties().getParse().getLodcloud().isStream();
		parseRE3 = properties.getIdolproperties().getParse().getRe3().isStream();
		parseCKANRepositories = properties.getIdolproperties().getParse().getCkanrepositories().isStream();
		parseLodStats = properties.getIdolproperties().getParse().getLodstats().isStream();
		parseLinghub = properties.getIdolproperties().getParse().getLinghub().isStream();
	}

}