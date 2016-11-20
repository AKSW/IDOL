/**
 * 
 */
package lodVader.services.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

import lodVader.mongodb.collections.Resources.GeneralResourceDB;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 11, 2016
 */
public class GeneralResourceServices {
	
	/**
	 * Get a set of resourcesIDs described by the URL
	 * @param resourcesURL: the resources url
	 * @param collection: the collection which should be queries 
	 * @return a list is resources IDs
	 */
	public List<ObjectId> getSetOfResourcesID(List<String> resourcesURL, GeneralResourceDB.COLLECTIONS collection) {

		List<ObjectId> resourcesIDs = new ArrayList<>();

		BasicDBObject query = new BasicDBObject(GeneralResourceDB.URI,
				new BasicDBObject("$in", resourcesURL));
		GeneralResourceDB.getCollection(collection.toString())
				.find(query).forEach((object) -> {
					resourcesIDs.add(new ObjectId(object.get(GeneralResourceDB.ID).toString()));
				});

		return resourcesIDs;
	}
	
	
	public List<String> getSetOfResourcesURL(List<ObjectId> resourcesIDs, GeneralResourceDB.COLLECTIONS collection) {

		List<String> resourcesURLs = new ArrayList<>();

		BasicDBObject query = new BasicDBObject(GeneralResourceDB.ID,
				new BasicDBObject("$in", resourcesIDs));
		GeneralResourceDB.getCollection(collection.toString())
				.find(query).forEach((object) -> {
					resourcesURLs.add(object.get(GeneralResourceDB.URI).toString());
				});

		return resourcesURLs;
	}
	
	

}
