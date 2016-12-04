package lodVader.mongodb.collections.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;

/**
 * Mapping for RDF resources (e.g. subjects, predicates...) and distributions/datasets.
 * @author Ciro Baron Neto
 * 
 * Oct 3, 2016
 */
public class GeneralResourceRelationDB extends DBSuperClass {

	public static final String PREDICATE_ID = "predicateID";

	public static final String DISTRIBUTION_ID = "distributionID";

	public static final String DATASET_ID = "datasetID";

	public static final String AMOUNT = "amount";

	
	// Enum for the name of the collections which will use this class
	public static enum COLLECTIONS {
		RELATION_ALL_PREDICATES, RELATION_RDF_TYPE, RELATION_OWL_CLASS, RELATION_RDF_SUBCLASS, RELATION_SUBJECT_NS, RELATION_OBJECT_NS, RELATION_SUBJECT_NS0, RELATION_OBJECT_NS0;
	};

	public COLLECTIONS collection;


	public GeneralResourceRelationDB(COLLECTIONS collection, DBObject obj) {
		super(collection.toString(), obj);
		setParameters();
		this.collection = collection;
		addField(ID, new ObjectId(obj.get(ID).toString()));
	}
//
//	
	public GeneralResourceRelationDB(COLLECTIONS collection) {
		super(collection.toString());
		setParameters();
		this.collection = collection;

	}

	private void setParameters() {
		addMandatoryField(PREDICATE_ID);
		addMandatoryField(DISTRIBUTION_ID);
		addMandatoryField(AMOUNT);
	}

	public String getPredicateID() {
		return getField(PREDICATE_ID).toString();
	}

	public void setPredicateID(int predicateID) {
		addField(PREDICATE_ID, predicateID);
	}

	public String getDistributionID() {
		return getField(DISTRIBUTION_ID).toString();
	}

	public void setDistributionID(String distributionID) {
		addField(DISTRIBUTION_ID, distributionID);
	}
	
	public String getDatasetID() {
		return getField(DATASET_ID).toString();
	}

	public void setDatasetID(String datasetID) {
		addField(DATASET_ID, datasetID);
	}

	public int getAmount() {
		return Integer.parseInt(getField(AMOUNT).toString());
	}

	public void setAmount(int amount) {
		addField(AMOUNT, amount);
	}

	public void insertSet(HashMap<String, Integer> set, List<GeneralResourceDB> resources, String distributionLODVaderID){
		
		// first check whether the relations exists, then update the values
		
		
		
		List<DBObject> objects = new ArrayList<>();
		for(GeneralResourceDB resource : resources){ 
			DBObject object = new BasicDBObject();
			object.put(DISTRIBUTION_ID, distributionLODVaderID);
			object.put(PREDICATE_ID, resource.getID());
			object.put(AMOUNT, set.get(resource.getUri()));
			objects.add(object);
		}
		
		bulkSave(objects);
	}
	
	
	public boolean bulkSave(List<DBObject> objects) {
		boolean isAck = false;
		try {
			if (objects.size() == 0)
				return false;
			BulkWriteOperation builder = getCollection().initializeUnorderedBulkOperation();
			for (DBObject doc : objects) {
				// and
				List<DBObject> find = new ArrayList<>();
				find.add(new BasicDBObject(DISTRIBUTION_ID, doc.get(DISTRIBUTION_ID)));
				find.add(new BasicDBObject(PREDICATE_ID, doc.get(PREDICATE_ID)));
				DBObject and = new BasicDBObject();
				and.put("$and", find);
				
				builder.find(and).upsert()
					.update(new BasicDBObject("$inc", new BasicDBObject(AMOUNT, doc.get(AMOUNT))));
			}
			BulkWriteResult result = builder.execute();
			isAck = result.isAcknowledged();

		} catch (BulkWriteException e) {
			e.printStackTrace();
		}
		return isAck;
	}
	
}
