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
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 14, 2016
 */
public class Fix {

	public static void main(String[] args) {

		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0.toString(),
				GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0.toString());
		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS.toString(),
				GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS.toString());
//
		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0.toString(),
				GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS0.toString());
		removeBlankNodes(GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS.toString(),
				GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS.toString());
//		
		

	}

	public static void removeBlankNodes(String resource_collection, String relation_collection) {

		List<DBObject> relationIDs = new ArrayList<>();
		List<DBObject> resourceIDs = new ArrayList<>();

		List<DBObject> objects = new GeneralQueriesHelper().getObjects(resource_collection,
				new BasicDBObject(GeneralResourceDB.URI, new BasicDBObject("$regex", "^(?!http).+")));

		while (objects.size() > 0) {
			for (DBObject object : objects) {
				relationIDs
						.add((new BasicDBObject("predicateID", new ObjectId(object.get("_id").toString()).toString())));
				resourceIDs.add((new BasicDBObject("_id", new ObjectId(object.get("_id").toString()))));
			}

			removeObjects(relationIDs, resourceIDs, resource_collection,relation_collection);

			objects = new GeneralQueriesHelper().getObjects(resource_collection,
					new BasicDBObject(GeneralResourceDB.URI, new BasicDBObject("$regex", "^(?!http).+")), 10000);
		}

	}

	public static void removeObjects(List<DBObject> relationIDs, List<DBObject> resourceIDs, String resource_collection, String relation_collection) {
		new DBSuperClass(relation_collection).bulkRemove(relationIDs);
		relationIDs = new ArrayList<>();
		new DBSuperClass(resource_collection).bulkRemove(resourceIDs);
		resourceIDs = new ArrayList<>();
	}

}
