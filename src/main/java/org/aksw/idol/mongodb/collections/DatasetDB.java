package org.aksw.idol.mongodb.collections;

import java.util.ArrayList;
import java.util.Collection;

import org.aksw.idol.mongodb.DBSuperClass;
import org.aksw.idol.mongodb.queries.DatasetQueries;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.DBObject;

public class DatasetDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "Dataset";

	public static final String PARENT_DATASETS = "parentDatasets";

	public static final String SUBSET_IDS = "subsetIDs";

	public static final String PROVENANCE = "provenance";

	public static final String DISTRIBUTIONS_IDS = "distributionsIDs";

	public static final String OBJECT_FILENAME = "objectFileName";

	public static final String SUBJECT_FILTER_FILENAME = "subjectFileName";
	
	public static final String URI = "uri";

	public static final String IS_VOCABULARY = "isVocabulary";

	public static final String TITLE = "title";

	public static final String LABEL = "label";

	public String provinance;


	public DatasetDB(DBObject object) {
		super(COLLECTION_NAME);
		setKeys();
		mongoDBObject = object;
	}
	
	/**
	 * Constructor for Class DatasetDB 
	 */
	public DatasetDB() {
		super(COLLECTION_NAME);
		setKeys();
	}
	
	/**
	 * Constructor for Class DatasetDB 
	 */
	public DatasetDB(String uri) {
		super(COLLECTION_NAME);
		setKeys();
		setUri(uri);
		find(true, URI, uri);
	}
	

	public void setKeys() {
		addMandatoryField(URI);
		addMandatoryField(PROVENANCE);
		
//		addMandatoryField(SUBSET_IDS);
//		addMandatoryField(DISTRIBUTIONS_IDS);
	}

	public void setSubsetIds(ArrayList<Integer> ids) {
		addField(SUBSET_IDS, ids);
	}

	public void setDistributionsIds(ArrayList<String> ids) {
		addField(DISTRIBUTIONS_IDS, ids);
	}
	
	/**
	 * @param provenance 
	 * Set the provenance value.
	 */
	public void addProvenance(String provinance) {
		ArrayList<String> ids = (ArrayList<String>) getField(PROVENANCE);
		if (ids != null) {
			if (!ids.contains(provinance)) {
				ids.add(provinance);
				addField(PROVENANCE, ids); 
			}
		} else {
			ids = new ArrayList<String>();
			ids.add(provinance);
			addField(PROVENANCE, ids);
		}
	}
	
	/**
	 * @return the provenance
	 */
	public String getProvenance() {
		return getField(PROVENANCE).toString();
	}

	public void addSubsetID(String id) {
		ArrayList<String> ids = (ArrayList<String>) getField(SUBSET_IDS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				addField(SUBSET_IDS, ids);
			}
		} else {
			ids = new ArrayList<String>(); 
			ids.add(id);
			addField(SUBSET_IDS, ids);
		}
	}

	public void addDistributionID(String id) {
		ArrayList<String> ids = (ArrayList<String>) getField(DISTRIBUTIONS_IDS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				addField(DISTRIBUTIONS_IDS, ids);
			}
		} else {
			ids = new ArrayList<String>();
			ids.add(id);
			addField(DISTRIBUTIONS_IDS, ids);
		}
	}
	
	public void setDistributionIDs(Collection<String> distributionIDs){
		for(String id : distributionIDs){
			addDistributionID(id);
		}
	}

	public ArrayList<String> getDistributionsIDs() {
		return (ArrayList<String>) getField(DISTRIBUTIONS_IDS);
	} 

	@JsonIgnore
	public ArrayList<DistributionDB> getDistributionsAsMongoDBObjects() {
		return new DatasetQueries().getDistributions(this);
	}

	public ArrayList<String> getSubsetsIDs() {
		return (ArrayList<String>) getField(SUBSET_IDS);
	}

	@JsonIgnore
	public ArrayList<DatasetDB> getSubsetsAsMongoDBObject() {
		return new DatasetQueries().getSubsetsAsMongoDBObject(this);
	}

	
	public ArrayList<Integer> getParentDatasetID() {
		try{
		ArrayList<Integer> parentDatasetsIDs = (ArrayList<Integer>) getField(PARENT_DATASETS);
		if (parentDatasetsIDs.get(0) != 0 || parentDatasetsIDs.size() >= 1)
			return parentDatasetsIDs;
		else
			return new ArrayList<Integer>();
		}
		catch(NullPointerException e){
			return new ArrayList<Integer>();
		}
	}

	public void addParentDatasetID(String id) {
		ArrayList<String> ids = (ArrayList<String>) getField(PARENT_DATASETS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				addField(PARENT_DATASETS, ids);
			}
		} else {
			ids = new ArrayList<String>();
			ids.add(id);
			addField(PARENT_DATASETS, ids);
		}
	}

//	public int getDatasetTriples() {
//		BasicDBObject query = new BasicDBObject(DistributionDB.TOP_DATASET, getID());
//		DBCursor list = getCollection(DistributionDB.COLLECTION_NAME).find(query);
//		int triples = 0;
//		for (DBObject d : list) {
//			if (d.get(DistributionDB.TRIPLES) != null)
//				triples = triples + ((Number) d.get(DistributionDB.TRIPLES)).intValue();
//		}
//		return triples;
//	}
	
	public void setIsVocabulary(boolean isVocabulary) {
		addField(IS_VOCABULARY, isVocabulary);
	}

	public String getTitle() {
		try {
			return getField(TITLE).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public boolean getIsVocabulary() {
		return Boolean.getBoolean(getField(IS_VOCABULARY).toString());
	}

	public void setTitle(String title) {
		addField(TITLE, title);
	}

	public void setLabel(String label) {
		addField(LABEL, label);
	}

	public String getLabel() {
		try {
			return getField(LABEL).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public String getUri() {
		return getField(URI).toString();
	}

	public void setUri(String uri) {
		addField(URI, uri);
	}
	

}
