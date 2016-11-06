/**
 * 
 */
package lodVader.spring.REST.models;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;

/**
 * Model which represents the streaming process status.
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 30, 2016
 */
public class DataSourceSizeRESTModel {

	private DecimalFormat formatter = new DecimalFormat("###,###,###,###,###,###");



	// mapping of datasources and datasourcestatus
	public HashMap<String, Double>  datasourcesSize = new Ds();
	
	class Ds extends HashMap<String, Double>{
		/* (non-Javadoc)
		 * @see java.util.HashMap#get(java.lang.Object)
		 */
		public String getSize(Object key) {
			// TODO Auto-generated method stub
			return formatter.format(super.get(key));
		}
	}
	

	/**
	 * Method which iterates over all distributions and create the hashmap of
	 * status separated out by datasources
	 * 
	 * @return
	 */
	public HashMap<String, Double> checkStatus() {

		// fetch all distributions
		ArrayList<DBObject> objects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		for (DBObject o : objects) {

			DistributionDB distributionDB = new DistributionDB(o);

			// for each datasource within the distribution, start or accumulate
			// the numbers
			for (String datasource : distributionDB.getDatasources()) {
				
				String filePath = LODVaderProperties.BASE_PATH + "/raw_files/__RAW_"+distributionDB.getID();
				
				File f = new File(filePath);
				double bytes = f.length()/1024/1024;

				if (datasourcesSize.get(datasource) == null) {
					datasourcesSize.put(datasource, bytes);
				} else {
					datasourcesSize.put(datasource, datasourcesSize.get(datasource) + bytes);
				}
			}
		}

		return datasourcesSize;
	}


}
