/**
 * 
 */
package idol.services.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

import idol.mongodb.collections.Resources.GeneralResourceDB;
import idol.mongodb.collections.Resources.GeneralResourceRelationDB;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class GeneralResourceRelationServices {

	/**
	 * Get a set of resources described by a distribution
	 * 
	 * @param distriutionID:
	 *            the distribution ID
	 * @param collection:
	 *            the collection which should be queries
	 * @return a list is resources IDs
	 */
	public List<ObjectId> getSetOfResourcesID(String distriutionID, GeneralResourceRelationDB.COLLECTIONS collection) {

		List<ObjectId> resourcesIDs = new ArrayList<>();

		BasicDBObject query = new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distriutionID);
		GeneralResourceRelationDB.getCollection(collection.toString()).find(query).forEach((object) -> {
			resourcesIDs.add(new ObjectId(object.get(GeneralResourceRelationDB.PREDICATE_ID).toString()));
		});

		return resourcesIDs;
	}

	/**
	 * Get a set of resources described by a distribution
	 * 
	 * @param distriutionID:
	 *            the distribution ID
	 * @param collection:
	 *            the collection which should be queries
	 * @return a list is resources IDs
	 */
	public List<String> getSetOfResourcesIDAsString(String distriutionID,
			GeneralResourceRelationDB.COLLECTIONS collection) {

		List<String> resourcesIDs = new ArrayList<>();

		BasicDBObject query = new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distriutionID);
		GeneralResourceRelationDB.getCollection(collection.toString()).find(query).forEach((object) -> {
			if (((Number) object.get(GeneralResourceRelationDB.AMOUNT)).intValue() >= 500)
				resourcesIDs.add(object.get(GeneralResourceRelationDB.PREDICATE_ID).toString());
		});

		return resourcesIDs;
	}

	/**
	 * Get a list of distribution ids based on the resources ids
	 * 
	 * @param resourcesID
	 * @param collection
	 * @return a map with distributionIDs as key and namespaces as values
	 */
	public HashMap<String, List<String>> getCommonDistributionsByResourceID(List<String> resourcesID,
			GeneralResourceRelationDB.COLLECTIONS collection) {

		HashMap<String, List<String>> distributionsIDs = new HashMap<>();

		// get datasets which describe common namespaces (target datasets)
		BasicDBObject query = new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID,
				new BasicDBObject("$in", resourcesID));
		GeneralResourceRelationDB.getCollection(collection.toString()).find(query).forEach((object) -> {
			String dist = object.get(GeneralResourceRelationDB.DISTRIBUTION_ID).toString();
			String ns = object.get(GeneralResourceRelationDB.PREDICATE_ID).toString();

			// filter distributions which contains at least 50 resources
			if (((Number) object.get(GeneralResourceRelationDB.AMOUNT)).intValue() >= 500)
				if (distributionsIDs.get(dist) == null) {
					List<String> namespaces = new ArrayList<>();
					namespaces.add(ns);
					distributionsIDs.put(dist, namespaces);
				} else {
					distributionsIDs.get(dist).add(ns);
				}
		});

		return distributionsIDs;
	}

}
