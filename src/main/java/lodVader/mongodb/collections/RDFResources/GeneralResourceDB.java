package lodVader.mongodb.collections.RDFResources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;

public class GeneralResourceDB extends DBSuperClass {

	public static final String URI = "uri";

	public static enum COLLECTIONS {
		RESOURCES_ALL_PREDICATES, RESOURCES_RDF_TYPE, RESOURCES_OWL_CLASS, RESOURCES_RDF_SUBCLASS, RESOURCES_OBJECT_NS0, RESOURCES_SUBJECT_NS0, RESOURCES_OBJECT_NS, RESOURCES_SUBJECT_NS
	};

	public COLLECTIONS collection;

	public GeneralResourceDB(COLLECTIONS collection) {
		super(collection.toString());
		this.collection = collection;
		setVariables();
	}

	public GeneralResourceDB(COLLECTIONS collection, String uri) {
		super(collection.toString());
		setVariables();
	}
	
	public GeneralResourceDB(COLLECTIONS collection, DBObject object) {
		super(collection.toString());
		setVariables();
		mongoDBObject = object;
	}

	public void setVariables() {
		addMandatoryField(URI);
	}

	public String getUri() {
		return getField(URI).toString();
	}

	public void setUri(String uri) {
		addField(URI, uri);
	}

	public List<GeneralResourceDB> insertSet(Set<String> set) {
		List<GeneralResourceDB> resources = new ArrayList<>();

		HashMap<String, DBObject> results = new HashMap<>();

		Iterator<String> i = set.iterator();

		while (i.hasNext()) {
			String resource = i.next();
			BasicDBObject obj = new BasicDBObject();
			obj.put(URI, resource);
			results.put(resource, obj);
		}

		bulkSave(new ArrayList<>(results.values()));
		bulkFind(results);

		for(DBObject obj : results.values()){
			resources.add(new GeneralResourceDB(collection, obj));
		}
		
		return resources;
	}

	public void bulkFind(HashMap<String, DBObject> resources) {

		DBCursor result = getCollection().find(new BasicDBObject(URI, new BasicDBObject("$in", resources.keySet())));
		Iterator<DBObject> i = result.iterator();
		while (i.hasNext()) {
			DBObject obj = i.next();
			resources.put(obj.get(URI).toString(), obj);
		}
	}

	public boolean bulkSave(List<DBObject> objects) {
		boolean isAck = false;
		try {
			if (objects.size() == 0)
				return false;
			BulkWriteOperation builder = getCollection().initializeUnorderedBulkOperation();
			for (DBObject doc : objects) {
				builder.find(new BasicDBObject(URI, doc.get(URI))).upsert().update(new BasicDBObject("$set", doc));
			}
			BulkWriteResult result = builder.execute();
			isAck = result.isAcknowledged();

		} catch (BulkWriteException e) {

		}
		return isAck;
	}

}
