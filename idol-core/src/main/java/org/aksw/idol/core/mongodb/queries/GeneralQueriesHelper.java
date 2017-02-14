package org.aksw.idol.core.mongodb.queries;

import java.util.ArrayList;

import org.aksw.idol.core.mongodb.DBSuperClass;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

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
		return getObjects(collectionName, new BasicDBObject(field, value), null, null, 1);
	}

	public ArrayList<DBObject> getObjects(String collectionName, String field, String value, String sort) {
		return getObjects(collectionName, new BasicDBObject(field, value), null, sort, 1);
	}

	public ArrayList<DBObject> getObjects(String collectionName, DBObject query) {
		return getObjects(collectionName, query, null, null, 1);
	}

	/**
	 * Get an array of MongoDB objects.
	 * 
	 * @param collectionName
	 * @param object
	 *            query
	 * @return array of DBObject
	 */
	public ArrayList<DBObject> getObjects(String collectionName, DBObject query, Integer limit, String sort,
			int sortVal) {

		ArrayList<DBObject> list = new ArrayList<DBObject>();
		
		try {
			DBCollection collection = DBSuperClass.getCollection(collectionName);
			DBCursor instances;
			if (limit == null) {
				if (sort == null)
					instances = collection.find(query);
				else
					instances = collection.find(query).sort(new BasicDBObject(sort, sortVal));

			} else {
				if (sort == null)
					instances = collection.find(query).limit(limit);
				else
					instances = collection.find(query).limit(limit).sort(new BasicDBObject(sort, sortVal));
			}

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
