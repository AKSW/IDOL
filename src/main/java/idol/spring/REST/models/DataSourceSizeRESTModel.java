/**
 * 
 */
package idol.spring.REST.models;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import idol.loader.LODVaderProperties;
import idol.mongodb.collections.DistributionDB;
import idol.mongodb.collections.datasetBF.BucketDB;
import idol.mongodb.queries.GeneralQueriesHelper;
import idol.services.mongodb.BucketService;

/**
 * Model which represents the streaming process status.
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 30, 2016
 */
public class DataSourceSizeRESTModel {


	// mapping of datasources and datasourcestatus
	public HashMap<String, SizeMap> datasourcesSize = new HashMap<String, SizeMap>();

	/**
	 * Method which iterates over all distributions and create the hashmap with
	 * datsources and their respective sizes
	 * 
	 * @return the hashmap with the datasources and their uncompressed sizes
	 */
	public HashMap<String, SizeMap> checkStatus() {

		// fetch all distributions
		ArrayList<DBObject> objects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		for (DBObject o : objects) {

			DistributionDB distributionDB = new DistributionDB(o);

			// for each datasource within the distribution, start the map and
			// sum up the sizes
			for (String datasource : distributionDB.getDatasources()) {

				/**
				 * Getting the uncompressed size
				 */
				String filePath = LODVaderProperties.BASE_PATH + "/raw_files/__RAW_" + distributionDB.getID();
				File f = new File(filePath);
				long bytes = f.length();

				/**
				 * Getting the BF size
				 */
				long bfSize = new BucketService()
						.getBucket(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES, distributionDB.getID(), false)
						.getBfByteSize();

				/**
				 * updating the return map
				 */
				if (datasourcesSize.get(datasource) == null) {
					SizeMap m = new SizeMap();
					m.setUncompressedSize((bytes));
					m.setBFSize((bfSize));
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
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Nov 18, 2016
	 */
	class SizeMap {
		long uncompressedSize;
		long BFSize;

		/**
		 * @return the bFSize
		 */
		@JsonIgnore
		public long getBFSize() {
			return BFSize;
		}
		
		/**
		 * @return the bFSize
		 */
		public String getFormattedBFSize() {
			DecimalFormat formatter = new DecimalFormat("###,###,###,###,###.###");
			return formatter.format(BFSize);
		}

		/**
		 * @param bFSize
		 *            Set the bFSize value.
		 */
		public void setBFSize(long bFSize) {
			BFSize = bFSize;
		}

		/**
		 * @return the uncompressedSize
		 */
		@JsonIgnore
		public long getUncompressedSize() {
			return uncompressedSize;
		}
		
		/**
		 * @return the uncompressedSize
		 */
		public String getFormattedUncompressedSize() {
			DecimalFormat formatter = new DecimalFormat("###,###,###,###,###.###");
			return formatter.format(uncompressedSize);
		}

		/**
		 * @param uncompressedSize
		 *            Set the uncompressedSize value.
		 */
		public void setUncompressedSize(long uncompressedSize) {
			this.uncompressedSize = uncompressedSize;
		}

	}

}
