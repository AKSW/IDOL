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
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.mongodb.collections.datasetBF.BucketService;
import lodVader.mongodb.queries.GeneralQueriesHelper;

/**
 * Model which represents the streaming process status.
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 30, 2016
 */
public class DataSourceSizeRESTModel {

	private DecimalFormat formatter = new DecimalFormat("###,###,###,###,###.###");

	// mapping of datasources and datasourcestatus
	public HashMap<String, SizeMap>  datasourcesSize = new HashMap<String, SizeMap> ();
	
	/**
	 * Method which iterates over all distributions and create the hashmap with datsources and their respective sizes
	 * 
	 * @return the hashmap with the datasources and their uncompressed sizes
	 */
	public HashMap<String, SizeMap> checkStatus() {

		// fetch all distributions
		ArrayList<DBObject> objects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		for (DBObject o : objects) {

			DistributionDB distributionDB = new DistributionDB(o);

			// for each datasource within the distribution, start the map and sum up the sizes
			for (String datasource : distributionDB.getDatasources()) {
				
				/**
				 * Getting the uncompressed size
				 */
				String filePath = LODVaderProperties.BASE_PATH + "/raw_files/__RAW_"+distributionDB.getID();
				File f = new File(filePath);
				double bytes = f.length()/1024/1024;
				
				/**
				 * Getting the BF size (in MB)
				 */
				Double bfSize = (double) ((new BucketService().getBucket(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES, distributionDB.getID(), true).getBfByteSize())
						/1024/1024);

				/**
				 * updating the return map
				 */
				if (datasourcesSize.get(datasource) == null) {
					SizeMap m = new SizeMap();
					m.setUncompressedSize(bytes);
					m.setBFSize(bfSize);
					datasourcesSize.put(datasource, m);
				} else {
					SizeMap m = datasourcesSize.get(datasource);
					m.setUncompressedSize(m.getUncompressedSize() + bytes);
					m.setBFSize(m.getBFSize() + bfSize);
				}
			}
		}

		return datasourcesSize;
	}

	
	/**
	 * Class representing the datasources sizes and BF sizes
	 * @author Ciro Baron Neto
	 * 
	 * Nov 18, 2016
	 */
	class SizeMap {
		Double uncompressedSize = 0.0;
		Double BFSize = 0.0;
		
		/**
		 * @return the bFSize
		 */
		public Double getBFSize() {
			return BFSize;
		}
		
		/**
		 * @param bFSize 
		 * Set the bFSize value.
		 */
		public void setBFSize(Double bFSize) {
			BFSize = bFSize;
		}
		
		/**
		 * @return the uncompressedSize
		 */
		public Double getUncompressedSize() {
			return uncompressedSize;
		}
		
		/**
		 * @param uncompressedSize 
		 * Set the uncompressedSize value.
		 */
		public void setUncompressedSize(Double uncompressedSize) {
			this.uncompressedSize = uncompressedSize;
		}
		
	}
	

}
