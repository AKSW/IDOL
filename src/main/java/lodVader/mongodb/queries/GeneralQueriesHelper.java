package lodVader.mongodb.queries;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;

public class GeneralQueriesHelper {

	/**
	 * Get an array of MongoDB objects.
	 * 
	 * @param collectionName
	 * @param field
	 * @param value
	 * @return array of DBObject
	 */
	public ArrayList<DBObject> getObjects(String collectionName, String field, String value) {
		return getObjects(collectionName, new BasicDBObject(field, value), null);
	}

	public ArrayList<DBObject> getObjects(String collectionName, DBObject query) {
		return getObjects(collectionName, query, null);
	}

	/**
	 * Get an array of MongoDB objects.
	 * 
	 * @param collectionName
	 * @param object
	 *            query
	 * @return array of DBObject
	 */
	public ArrayList<DBObject> getObjects(String collectionName, DBObject query, Integer limit) {

		ArrayList<DBObject> list = new ArrayList<DBObject>();
		try {
			DBCollection collection = DBSuperClass.getCollection(collectionName);
			DBCursor instances;
			if (limit == null)
				instances = collection.find(query);
			else
				instances = collection.find(query).limit(limit);

			for (DBObject instance : instances) {
				list.add(instance);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Remove documents of a collection
	 * 
	 * @param collectionName
	 * @param field
	 * @param value
	 */
	public void removeObjects(String collectionName, String field, String value) {
		DBSuperClass.getCollection(collectionName).remove(new BasicDBObject(field, value));
	}

}
