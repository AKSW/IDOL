package lodVader.mongodb.collections.RDFResources;

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
public class GeneralRDFResourceRelationDB extends DBSuperClass {

	public static final String PREDICATE_ID = "predicateID";

	public static final String DATASET_ID = "datasetID";

	public static final String DISTRIBUTION_ID = "distributionID";

	public static final String AMOUNT = "amount";

	// ID which consists in the DatabaseID, DistributionID and ResourceID.
	// the format is : DatabaseID-DistributionID-ResourceID
	public static final String CUSTOM_ID = "custom_id";

	
	// Enum for the name of the collections which will use this class
	public static enum COLLECTIONS {
		RELATION_ALL_PREDICATES, RELATION_RDF_TYPE, RELATION_OWL_CLASS, RELATION_RDF_SUBCLASS;
	};

	public COLLECTIONS collection;


//	public GeneralRDFResourceRelationDB(COLLECTIONS collection, DBObject obj) {
//		super(collection.toString(), obj);
//		setParameters();
//		this.collection = collection;
//	}
//
//	
	public GeneralRDFResourceRelationDB(COLLECTIONS collection) {
		super(collection.toString());
		setParameters();
		this.collection = collection;

	}

	private void setParameters() {
		addMandatoryField(PREDICATE_ID);
		addMandatoryField(DATASET_ID);
		addMandatoryField(DISTRIBUTION_ID);
		addMandatoryField(AMOUNT);
	}

	public String getCustomId() {
		return getField(CUSTOM_ID).toString();
	}

	public String getPredicateID() {
		return getField(PREDICATE_ID).toString();
	}

	public void setPredicateID(int predicateID) {
		addField(PREDICATE_ID, predicateID);
	}

	public void setCustomId(String id) {
		addField(CUSTOM_ID, id);
	}

	public int getDatasetID() {
		return Integer.parseInt(getField(DATASET_ID).toString());
	}

	public void setDatasetID(int datasetID) {
		addField(DATASET_ID, datasetID);
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

	public void insertSet(HashMap<String, Integer> set, List<GeneralRDFResourceDB> resources, String distributionLODVaderID, String topDatasetLODVaderID){
		
		List<DBObject> objects = new ArrayList<>();
		for(GeneralRDFResourceDB resource : resources){ 
			DBObject object = new BasicDBObject();
			object.put(DATASET_ID, topDatasetLODVaderID);
			object.put(DISTRIBUTION_ID, distributionLODVaderID);
			object.put(PREDICATE_ID, resource.getID());
			object.put(AMOUNT, set.get(resource.getUri()));
			object.put(CUSTOM_ID, topDatasetLODVaderID+ "-"+distributionLODVaderID+ "-"+ resource.getID());
			objects.add(object);
		}
		
		bulkSave2(objects);
	}
}
