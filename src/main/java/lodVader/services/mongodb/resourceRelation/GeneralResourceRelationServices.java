/**
 * 
 */
package lodVader.services.mongodb.resourceRelation;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;

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
			resourcesIDs.add(object.get(GeneralResourceRelationDB.PREDICATE_ID).toString());
		});

		return resourcesIDs;
	}

	/**
	 * Get a list of distribution ids based on the resources ids
	 * 
	 * @param resourcesID
	 * @param collection
	 * @return a list of distributionIDs
	 */
	public List<String> getCommonDistributionsByResourceID(List<String> resourcesID,
			GeneralResourceRelationDB.COLLECTIONS collection) {

		List<String> distributionsIDs = new ArrayList<>();

		// get datasets which describe common namespaces (target datasets)
		BasicDBObject query = new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID,
				new BasicDBObject("$in", resourcesID));
		GeneralResourceRelationDB.getCollection(collection.toString()).find(query).forEach((object) -> {
			distributionsIDs.add(object.get(GeneralResourceRelationDB.DISTRIBUTION_ID).toString());
		});

		return distributionsIDs;
	}

}
