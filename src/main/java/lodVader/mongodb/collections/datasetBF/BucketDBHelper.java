/**
 * 
 */
package lodVader.mongodb.collections.datasetBF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.mongodb.DBSuperClass;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 9, 2016
 */
public class BucketDBHelper {

	public HashMap<String, List<BloomFilterI>> getDistributionFilters(BucketDB.COLLECTIONS collection, List<String> distributionsIDs) {

		// get all distributions within the dataset

		HashMap<String, List<BloomFilterI>> buckets = new HashMap<String, List<BloomFilterI>>();

		// get collection
		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), collection.toString());
		
		// create query
		BasicDBObject distributions = new BasicDBObject(BucketDB.DISTRIBUTION_ID, new BasicDBObject("$in", distributionsIDs));

		
		// make query
		List<GridFSDBFile> bucketsDB = gfs.find(distributions);

		for (GridFSDBFile f : bucketsDB) {
			BloomFilterI filter = BloomFilterFactory.newBloomFilter();
			try {
				filter.readFrom(f.getInputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (buckets.get(f.get(BucketDB.DISTRIBUTION_ID).toString()) == null) {
				List<BloomFilterI> list = new ArrayList<>();
				list.add(filter); 
				buckets.put(f.get(BucketDB.DISTRIBUTION_ID).toString(), list);
			}
			else{
				buckets.get(f.get(BucketDB.DISTRIBUTION_ID).toString()).add(filter);
			}
		}

		return buckets;
	}

}
