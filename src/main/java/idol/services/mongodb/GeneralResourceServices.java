/**
 * 
 */
package idol.services.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

import idol.mongodb.collections.Resources.GeneralResourceDB;

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
	
	/**
	 * Get a map having namespace ID and namespace URL
	 * @param resourcesIDs
	 * @param collection
	 * @return
	 */
	public HashMap<String, String> getSetOfResourcesInstances(List<String> resourcesIDs, GeneralResourceDB.COLLECTIONS collection) {

		 HashMap<String, String>resourcesURLs = new HashMap<>();

		BasicDBObject query = new BasicDBObject(GeneralResourceDB.ID,
				new BasicDBObject("$in", transformIDsToObjectIDs(resourcesIDs)));
		GeneralResourceDB.getCollection(collection.toString())
				.find(query).forEach((object) -> {
					GeneralResourceDB r = new GeneralResourceDB(collection, object);
					resourcesURLs.put(r.getID(), r.getUri());
				});

		return resourcesURLs;
	}
	
	private List<ObjectId> transformIDsToObjectIDs(List<String> ids){
		List<ObjectId> r = new ArrayList<>();
		for(String id : ids)
			r.add(new ObjectId(id));
		return r;
	}
	
	

}
