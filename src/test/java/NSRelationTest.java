import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Statement;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.loader.LODVaderConfigurator;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.tupleManager.processors.BloomFilterProcessor2;
import lodVader.utils.NSUtils;
import lodVader.utils.StatementUtils;

/**
 * 
 */

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 14, 2016
 */
public class NSRelationTest {
	NSUtils nsUtils = new NSUtils();

	@Test
	public void nsRelationTest() {
		/**
		 * Load properties file, create MondoDB indexes, etc
		 */
		LODVaderConfigurator s = new LODVaderConfigurator();
		s.configure();

		StatementUtils stmtUtils = new StatementUtils();
		DistributionDB distribution = InstancesFactoryTest.createDistribution();

		/**
		 * Create a set of statement
		 */
		Statement st1 = stmtUtils.createStatement("http://ns0.org/", "http://predicate.org", "http://object.org");
		Statement st2 = stmtUtils.createStatement("http://ns0.org/str1", "http://predicate.org", "http://object.org");
		Statement st3 = stmtUtils.createStatement("http://ns0.org/str2/asddd", "http://predicate.org", "http://object.org");
		Statement st4 = stmtUtils.createStatement("http://ns1.org/", "http://predicate.org", "http://object.org");
		Statement st5 = stmtUtils.createStatement("http://ns2.org/", "http://predicate.org", "http://object.org");

		/**
		 * Process them
		 */
		BloomFilterProcessor2 processor = new BloomFilterProcessor2(distribution);
		processor.process(st1);
		processor.process(st1);
		processor.process(st1);
		processor.process(st1);
		processor.process(st1);
		processor.process(st2);
		processor.process(st3);
		processor.process(st4);
		processor.process(st5);

		processor.saveFilters();

		// check if the ns was saved only once

		Assert.assertEquals(countResource("http://ns0.org"), 1);

		Assert.assertEquals(countResource("http://ns1.org"), 1);

		Assert.assertEquals(countResource("http://ns2.org"), 1);

		// now check the relations
		GeneralResourceDB st1r = getResource("http://ns0.org");

		GeneralResourceDB st4r = getResource("http://ns1.org");

		GeneralResourceDB st5r = getResource("http://ns2.org");

		List<DBObject> list = new ArrayList<>();
		list.add(new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distribution.getID()));
		list.add(new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID, st1r.getID()));

		Assert.assertEquals(countRelation(st1r.getID(), distribution.getID()), 1);

		Assert.assertEquals(countRelation(st4r.getID(), distribution.getID()), 1);

		Assert.assertEquals(countRelation(st5r.getID(), distribution.getID()), 1);

		Assert.assertEquals(getRelation(st1r.getID(), distribution.getID()).getAmount(), 7);
		
		removeRelation(st1r.getID(), distribution.getID());
		removeRelation(st4r.getID(), distribution.getID());
		removeRelation(st5r.getID(), distribution.getID());
		
		removeResource(st1r.getUri());
		removeResource(st4r.getUri());
		removeResource(st5r.getUri());

	}

	private GeneralResourceDB getResource(String resource) {
		return new GeneralResourceDB(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0,
				new GeneralQueriesHelper().getObjects(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0.toString(),
						GeneralResourceDB.URI, nsUtils.getNS0(resource)).iterator().next());
	}

	 private GeneralResourceRelationDB getRelation(String uriID, String distributionID){
		 List<DBObject> list = new ArrayList<>();
			list.add(new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distributionID));
			list.add(new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID, uriID));

			return new GeneralResourceRelationDB(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0,
					new GeneralQueriesHelper().getObjects(
							GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0.toString(),
							new BasicDBObject("$and", list)).iterator().next());
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
	
	private void removeResource(String resource){
		DBSuperClass.getCollection(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0.toString()).remove(
				new BasicDBObject(GeneralResourceDB.URI, resource));
	}
	
	private void removeRelation(String uriID, String distributionID){
		List<DBObject> list = new ArrayList<>();
		list.add(new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distributionID));
		list.add(new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID, uriID));
		DBSuperClass.getCollection(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0.toString()).remove(
				new BasicDBObject("$and", list));
	}

}
