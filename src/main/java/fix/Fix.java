/**
 * 
 */
package fix;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.utils.NSUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 14, 2016
 */
public class Fix {

//	ExecutorService ex = Executors.newFixedThreadPool(3);
	/**
	 * Constructor for Class Fix 
	 */
	public Fix() {

//		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0.toString(),
//				GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0.toString());
//		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS.toString(),
//				
//				GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS.toString());
//
//		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0.toString(),
//				GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS0.toString());
//		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS.toString(),
//				GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS.toString());
//		
		
		System.out.println("-- - - - - end");


	}
	
	
	
//	public void getNS(String resource_collection, String relation_collection){
//		
//		List<DBObject> objects = new GeneralQueriesHelper().getObjects(resource_collection,
//				new BasicDBObject(), null, "uri");
//		
//		
//		List<String> ids = new ArrayList<>();
//		NSUtils nsUtils = new NSUtils();
//		String lastURINS = nsUtils.getNSFromString(objects.iterator().next().get("uri").toString());
//		
//		for(DBObject object : objects){
//			String uri = object.get("uri").toString();
//			String ns = nsUtils.getNSFromString(uri);
//			if(ns.startsWith(lastURINS))
//				ids.add(object.get("_id").toString());
//			else{
//				// remove 
//				lastURINS = ns;
//			}
//				
//		}
//		
//		
//		
//		
//		
//	}
	
	
	

	public void removeBlankNodes(String resource_collection, String relation_collection) {

		List<DBObject> relationIDs = new ArrayList<>(); 
		List<DBObject> resourceIDs = new ArrayList<>();

		List<DBObject> objects = new GeneralQueriesHelper().getObjects(resource_collection,
				new BasicDBObject(GeneralResourceDB.URI, new BasicDBObject("$regex", "^(?!http).+")), 100000, null);
		

		int i = 0;
		while (objects.size() > 0) {
			System.out.println(i++ +" Loading...");
			for (DBObject object : objects) {
				relationIDs
						.add((new BasicDBObject("predicateID", new ObjectId(object.get("_id").toString()).toString())));
				resourceIDs.add((new BasicDBObject("_id", new ObjectId(object.get("_id").toString()))));
			}

//			ex.execute(new Remove(relationIDs, resourceIDs, resource_collection, relation_collection));
			removeObjects(relationIDs, resourceIDs, resource_collection,relation_collection);
 
			objects = new GeneralQueriesHelper().getObjects(resource_collection,
					new BasicDBObject(GeneralResourceDB.URI, new BasicDBObject("$regex", "^(?!http).+")), 100000, null);
		}
		

		System.out.println("end ");

	}
	
	

	public void removeObjects(List<DBObject> relationIDs, List<DBObject> resourceIDs, String resource_collection, String relation_collection) {
	System.out.println("removing relations...");
		new DBSuperClass(relation_collection).bulkRemove(relationIDs);
		relationIDs = new ArrayList<>();
		System.out.println("removing resources...");
		
		new DBSuperClass(resource_collection).bulkRemove(resourceIDs);
		resourceIDs = new ArrayList<>();
	}
	
	class Remove implements Runnable{
		
		List<DBObject> relationIDs;
		List<DBObject> resourceIDs;
		String resource_collection;
		String relation_collection;
		/**
		 * Constructor for Class Fix.Remove 
		 */
		public Remove(List<DBObject> relationIDs, List<DBObject> resourceIDs, String resource_collection, String relation_collection ) {
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
