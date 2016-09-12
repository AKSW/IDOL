/**
 * 
 */
package lodVader.mongodb.collections.datasetBF;

import java.io.IOException;

import lodVader.utils.bloomfilter.BloomFilterCache;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 12, 2016
 */
public class ServiceDatasetBFDB {

	public static void saveCache(BloomFilterCache cache, int distributionID) {
		new DatasetBFBucketDB().saveCache(cache, distributionID);

	}

	public static BloomFilterCache loadBfCache(int distributionID) {
		try {
			return new DatasetBFBucketDB().loadCache(distributionID);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
