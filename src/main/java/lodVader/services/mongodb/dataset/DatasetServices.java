/**
 * 
 */
package lodVader.services.mongodb.dataset;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.Dataset;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DatasetDB;

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
	public void saveAllDatasets(List<DatasetDB> datasets){
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

}
