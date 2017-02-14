/**
 * 
 */
package org.aksw.idol.core.services.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.aksw.idol.core.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.core.mongodb.DBSuperClass;
import org.aksw.idol.core.mongodb.collections.DatasetDB;
import org.aksw.idol.core.mongodb.queries.GeneralQueriesHelper;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author Ciro Baron Neto
 * 
 * Sep 11, 2016
 */
public class DatasetServices {
	
	/**
	 * Save all datasets into the MongoDB database
	 * @param datasets
	 */
	public void saveAllDatasets(Collection<DatasetDB> datasets){
		datasets.forEach((dataset)->{
			try {
				dataset.update();
			} catch (Exception e) {
				e.printStackTrace();
			} 
		});
	}
	
	
	/**
	 * Get all datasets
	 * @return list of datasets
	 */
	public List<DatasetDB> getDatasets(Boolean vocabularies) {

		List<DatasetDB> list = new ArrayList<DatasetDB>();
		try {
			DBCollection collection = DBSuperClass.getDBInstance().getCollection(
					DatasetDB.COLLECTION_NAME);
			
			DBCursor instances ;
			
			if(vocabularies==null)
				instances = collection.find();
			else if(vocabularies)
				instances = collection.find(new BasicDBObject(DatasetDB.IS_VOCABULARY, true));
			else
				instances = collection.find(new BasicDBObject(DatasetDB.IS_VOCABULARY, false));
			
			for (DBObject instance : instances) {
				list.add(new DatasetDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public boolean removeDataset(String datasetID){
		
		DBCollection collection = DBSuperClass.getDBInstance().getCollection(
				DatasetDB.COLLECTION_NAME);
		collection.remove(new BasicDBObject(DatasetDB.ID, datasetID));
		
		return true;	
	}
	
	
	/**
	 * Add a new dataset in the list. The method will save it and return an respective ID
	 * @param uri the dataset uri
	 * @param isVocab represents if the dataset is a vocabulary or a ontology
	 * @param title the dataset title
	 * @param labelthe dataset label
	 * @param provenance whe the dataset came from
	 * @return the dataset ID
	 */
	public DatasetDB saveDataset(String uri, boolean isVocab, String title, String label, String provenance){
		
		DatasetDB datasetDB  = null;
		
		/**
		 * Check if the dataset already exists in database. 
		 */
		ArrayList<DBObject> d = new GeneralQueriesHelper().getObjects(DatasetDB.COLLECTION_NAME, DatasetDB.URI, uri);
		if(d.size()>0){
			datasetDB = new DatasetDB(d.iterator().next());
		}
		
		/**
		 * Create case do not exists
		 */
		else{
			datasetDB = new DatasetDB(uri);
			datasetDB.setIsVocabulary(isVocab);
			datasetDB.setTitle(title);
			datasetDB.setLabel(label);
		}

		/**
		 * Add provenance 
		 */
		datasetDB.addProvenance(provenance);	
		
		/**
		 * Update object adding the provenance
		 */
		try {
			datasetDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			e.printStackTrace();
		}
		
		return datasetDB;
	}

}
