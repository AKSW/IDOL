import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Statement;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.loader.LODVaderConfigurator;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.plugins.intersection.subset.SubsetDetectionService;
import lodVader.plugins.intersection.subset.distribution.SubsetDistributionDetectionService;
import lodVader.services.mongodb.BucketService;
import lodVader.plugins.intersection.subset.distribution.SubsetDetectorBFIntersectImpl;
import lodVader.streaming.LODVStreamFileImpl;
import lodVader.streaming.LODVStreamInterface;
import lodVader.tupleManager.processors.BasicProcessorInterface;
import lodVader.tupleManager.processors.BloomFilterProcessor2;
import lodVader.tupleManager.processors.SaveDumpDataProcessor;
import lodVader.utils.NSUtils;
import lodVader.utils.StatementUtils;
import orestes.bloomfilter.json.BloomFilterConverter;

/**
 * 
 */

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 14, 2016
 */
public class BFTest {
	NSUtils nsUtils = new NSUtils();

	@Test
	public void nsRelationTest() {
		/**
		 * Load properties file, create MondoDB indexes, etc
		 */
		LODVaderConfigurator s = new LODVaderConfigurator();
		s.configure();

		StatementUtils stmtUtils = new StatementUtils();
		DistributionDB distribution1 = InstancesFactoryTest.createDistribution();
		DistributionDB distribution2 = InstancesFactoryTest.createDistribution();

		List<Statement> d1stmt = new ArrayList<>();
		List<Statement> d2stmt = new ArrayList<>();

		/**
		 * Create statements and save them
		 */
		for (int i = 0; i < 1_000; i++) {
			Statement st = stmtUtils.createStatement("http://ns0.org/" + generateString(), "http://predicate.org",
					"http://object.org/" + generateString());
			d1stmt.add(st);
			if (i % 2 == 0)
				d2stmt.add(st);
		}

		processDist(d1stmt, distribution1);
		processDist(d2stmt, distribution2);

		// intersection plugin

		LODVaderIntersectionPlugin subsetDetector = new SubsetDetectorBFIntersectImpl();
		SubsetDetectionService subsetService = new SubsetDistributionDetectionService(subsetDetector, distribution1);
		subsetService.saveSubsets();
		
		

		// SubsetDetectionService sub = new SubsetDetection

		/**
		 * Remove files
		 */

		new File(LODVaderProperties.BASE_PATH + "/raw_files/" + "__RAW_" + distribution1.getID()).delete();
		new File(LODVaderProperties.BASE_PATH + "/raw_files/" + "__RAW_" + distribution2.getID()).delete();
	}

	private void processDist(List<Statement> statements, DistributionDB distribution) {
		SaveDumpDataProcessor rawDataProcessor = new SaveDumpDataProcessor(distribution, distribution.getID());

		for (Statement st : statements)
			rawDataProcessor.process(st);

		rawDataProcessor.closeFile();

		BloomFilterProcessor2 processor = new BloomFilterProcessor2(distribution);
		LODVStreamInterface streamer = new LODVStreamFileImpl(LODVaderProperties.BASE_PATH + "/raw_files/");

		streamer.getPipelineProcessor().registerProcessor(processor);
		try {
			streamer.startParsing(distribution);
		} catch (Exception e) {
			e.printStackTrace();
		}
		processor.saveFilters();
	}

	private GeneralResourceDB getResource(String resource) {
		return new GeneralResourceDB(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0,
				new GeneralQueriesHelper().getObjects(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0.toString(),
						GeneralResourceDB.URI, nsUtils.getNS0(resource)).iterator().next());
	}

	private GeneralResourceRelationDB getRelation(String uriID, String distributionID) {
		List<DBObject> list = new ArrayList<>();
		list.add(new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distributionID));
		list.add(new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID, uriID));

		return new GeneralResourceRelationDB(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0,
				new GeneralQueriesHelper()
						.getObjects(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0.toString(),
								new BasicDBObject("$and", list))
						.iterator().next());
	}

	private int countRelation(String uriID, String distributionID) {
		List<DBObject> list = new ArrayList<>();
		list.add(new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distributionID));
		list.add(new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID, uriID));

		return new GeneralQueriesHelper()
				.getObjects(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0.toString(),
						new BasicDBObject("$and", list))
				.size();
	}

	private int countResource(String resource) {
		return new GeneralQueriesHelper().getObjects(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0.toString(),
				GeneralResourceDB.URI, nsUtils.getNS0(resource)).size();
	}

	private void removeResource(String resource) {
		DBSuperClass.getCollection(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0.toString())
				.remove(new BasicDBObject(GeneralResourceDB.URI, resource));
	}

	private void removeRelation(String uriID, String distributionID) {
		List<DBObject> list = new ArrayList<>();
		list.add(new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distributionID));
		list.add(new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID, uriID));
		DBSuperClass.getCollection(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0.toString())
				.remove(new BasicDBObject("$and", list));
	}

	public static String generateString()

	{
		Random rng = new Random();
		int length = 15;
		String characters = "qwertyuioplkjhgfdsazxcvbnm1234567890";
		char[] text = new char[length];
		for (int i = 0; i < length; i++) {
			text[i] = characters.charAt(rng.nextInt(characters.length()));
		}
		return new String(text);
	}

}
