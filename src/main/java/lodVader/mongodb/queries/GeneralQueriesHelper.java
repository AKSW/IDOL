package lodVader.mongodb.queries;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.ResourceDB;

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

		ArrayList<DBObject> list = new ArrayList<DBObject>();
		try {
			DBCollection collection = DBSuperClass.getCollection(collectionName);
			DBObject query = new BasicDBObject(field, value);
			DBCursor instances = collection.find(query);

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
	 * @param collectionName
	 * @param field
	 * @param value
	 */
	public void removeObjects(String collectionName, String field, String value) {
		WriteResult a = DBSuperClass.getCollection(collectionName).remove(new BasicDBObject(field, value));
	}

}
