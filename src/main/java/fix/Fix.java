/**
 * 
 */
package fix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.plaf.synth.SynthSpinnerUI;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.ckanparser.CkanResourceDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.services.mongodb.distribution.DistributionServices;
import lodVader.utils.NSUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 14, 2016
 */
public class Fix {

	// ExecutorService ex = Executors.newFixedThreadPool(3);
	/**
	 * Constructor for Class Fix
	 */
	public Fix() {
	}

	public void fix1() {

		List<DBObject> distributions = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		for (DBObject dist : distributions) {
			DistributionDB distribution = new DistributionDB(dist);
			if (distribution.getDownloadUrl().contains(".xls") || distribution.getDownloadUrl().contains(".doc")) {
				System.out.println(distribution.getDownloadUrl());
				new DistributionServices().removeDistribution(distribution, true);
			}
		}

	}

	public void fix2() {
		List<String> catalogs = new ArrayList<>(
				Arrays.asList("http://data.bris.ac.uk/data/", "http://open.canada.ca/data/en/",
						"http://catalog.data.gov/", "http://data.gov.au/", "https://datastore.landcareresearch.co.nz/",
						"https://datahub.io/", "https://iatiregistry.org/", "http://data.london.gov.uk/"));

		for (String catalog : catalogs) {

			for (DBObject obj : new GeneralQueriesHelper().getObjects(CkanResourceDB.COLLECTION_NAME,
					CkanResourceDB.DATASOURCE, catalog)) {
				System.out.println(new CkanResourceDB(obj).getCatalog());
			}
		}

	}

	public static void main(String[] args) {
		new Fix().fix2();

	}

	public void removeObjects(List<DBObject> relationIDs, List<DBObject> resourceIDs, String resource_collection,
			String relation_collection) {
		System.out.println("removing relations...");
		new DBSuperClass(relation_collection).bulkRemove(relationIDs);
		relationIDs = new ArrayList<>();
		System.out.println("removing resources...");

		new DBSuperClass(resource_collection).bulkRemove(resourceIDs);
		resourceIDs = new ArrayList<>();
	}

	class Remove implements Runnable {

		List<DBObject> relationIDs;
		List<DBObject> resourceIDs;
		String resource_collection;
		String relation_collection;

		/**
		 * Constructor for Class Fix.Remove
		 */
		public Remove(List<DBObject> relationIDs, List<DBObject> resourceIDs, String resource_collection,
				String relation_collection) {
			this.relation_collection = relation_collection;
			this.resource_collection = resource_collection;
			this.relationIDs = relationIDs;
			this.resourceIDs = resourceIDs;
		}

		public void run() {
			new DBSuperClass(relation_collection).bulkRemove(relationIDs);
			relationIDs = new ArrayList<>();
			new DBSuperClass(resource_collection).bulkRemove(resourceIDs);
			resourceIDs = new ArrayList<>();
		}
	}

}
