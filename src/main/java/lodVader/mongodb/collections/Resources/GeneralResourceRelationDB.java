package lodVader.mongodb.collections.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
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

	public static final String AMOUNT = "amount";

	
	// Enum for the name of the collections which will use this class
	public static enum COLLECTIONS {
		RELATION_ALL_PREDICATES, RELATION_RDF_TYPE, RELATION_OWL_CLASS, RELATION_RDF_SUBCLASS, RELATION_SUBJECT_NS, RELATION_OBJECT_NS, RELATION_SUBJECT_NS0, RELATION_OBJECT_NS0;
	};

	public COLLECTIONS collection;


//	public GeneralRDFResourceRelationDB(COLLECTIONS collection, DBObject obj) {
//		super(collection.toString(), obj);
//		setParameters();
//		this.collection = collection;
//	}
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

	public int getDistributionID() {
		return Integer.parseInt(getField(DISTRIBUTION_ID).toString());
	}

	public void setDistributionID(int distributionID) {
		addField(DISTRIBUTION_ID, distributionID);
	}

	public int getAmount() {
		return Integer.parseInt(getField(AMOUNT).toString());
	}

	public void setAmount(int amount) {
		addField(AMOUNT, amount);
	}

	public void insertSet(HashMap<String, Integer> set, List<GeneralResourceDB> resources, String distributionLODVaderID, String topDatasetLODVaderID){
		
		List<DBObject> objects = new ArrayList<>();
		for(GeneralResourceDB resource : resources){ 
			DBObject object = new BasicDBObject();
			object.put(DISTRIBUTION_ID, distributionLODVaderID);
			object.put(PREDICATE_ID, resource.getID());
			object.put(AMOUNT, set.get(resource.getUri()));
			objects.add(object);
		}
		
		bulkSave2(objects);
	}
}
