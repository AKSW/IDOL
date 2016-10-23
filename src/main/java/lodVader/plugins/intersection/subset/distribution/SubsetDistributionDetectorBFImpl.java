/**
 * 
 */
package lodVader.plugins.intersection.subset.distribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.mongodb.collections.datasetBF.BucketDBHelper;
import lodVader.plugins.LODVaderPlugin;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.services.mongodb.resourceRelation.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class SubsetDistributionDetectorBFImpl extends LODVaderIntersectionPlugin {

	public static String PLUGIN_NAME = "SUBSET_BLOOM_FILTER_DETECTOR";

	public static HashMap<String, List<BloomFilterI>> bfCache = new HashMap<>();

	public static AtomicBoolean loading = new AtomicBoolean(false);

	private static List<Object> threadList = new ArrayList<Object>();

	/**
	 * Constructor for Class SubsetDetectorBFImpl
	 * 
	 * @param pluginName
	 */
	public SubsetDistributionDetectorBFImpl() {
		super(PLUGIN_NAME);
	}

	public HashMap<String, List<BloomFilterI>>  loadBucketIntoCache(List<String> distributions) {

		// make all threads hold on for their time
		Object lock = new Object();
		synchronized (lock) {
			if (SubsetDistributionDetectorBFImpl.loading.get()) {
				SubsetDistributionDetectorBFImpl.threadList.add(this);
				try {
					synchronized (this) {
						this.wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else{
				SubsetDistributionDetectorBFImpl.loading.set(true);				
			}
		}
		
		
		// load 
		
		List<String> distributionsQuery = new ArrayList<String>();
		for (String distribution : distributions) {
			if (!bfCache.containsKey(distribution)) {
				distributionsQuery.add(distribution);
			}
		}

		// make query
		HashMap<String, List<BloomFilterI>> queryResult = new BucketDBHelper()
				.getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES, distributionsQuery);

		// update cache list
		for (String id : queryResult.keySet()) {
			bfCache.put(id, queryResult.get(id));
		}
		
		
		// notify nxt thread
		if(threadList.size()>0){
			Object consumerLock = threadList.iterator().next();
			synchronized (consumerLock) {
				threadList.remove(consumerLock);
				consumerLock.notify();
			}
		}
		else{
			loading.set(false);
		}
		
		return bfCache;
	}

//	private HashMap<String, List<BloomFilterI>> getBucketFromDatasets(List<String> distributions) {
//
//		List<String> removeList = new ArrayList<String>();
//
//		// remove unused BFs from the cache
//		// for (String id : bfCache.keySet()) {
//		// if (!distributions.contains(id)) {
//		// removeList.add(id);
//		// }
//		// }
//		// for(String remove: removeList){
//		// bfCache.remove(remove);
//		// }
//
//		// make sure that we are not loading the same BF 2 times in a row
//		List<String> distributionsQuery = new ArrayList<String>();
//		for (String distribution : distributions) {
//			if (!bfCache.containsKey(distribution)) {
//				distributionsQuery.add(distribution);
//			}
//		}
//
//		// make query
//		HashMap<String, List<BloomFilterI>> queryResult = new BucketDBHelper()
//				.getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES, distributionsQuery);
//
//		// update cache list
//		for (String id : queryResult.keySet()) {
//			bfCache.put(id, queryResult.get(id));
//		}
//
//		return bfCache;
//
//	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.application.subsetdetection.SubsetDetectionI#detectSubsets()
	 */
	@Override
	public HashMap<String, Double> runDetection(DistributionDB sourceDistribution,
			List<String> targetDistributionsIDs) {

		HashMap<String, Double> returnMap = new HashMap<String, Double>();
		System.out.println(targetDistributionsIDs.size() + " target distributions found.");

		// load the buckets of the source and the target distriution
//		HashMap<String, List<BloomFilterI>> distributionsBF = getBucketFromDatasets(targetDistributionsIDs);
		HashMap<String, List<BloomFilterI>> distributionsBF = loadBucketIntoCache(targetDistributionsIDs);

		// get BF from the source distribution
		List<BloomFilterI> mainDistributionBFs = distributionsBF.get(sourceDistribution.getID());

		// compare all bfs
		for (String targetDistribution : targetDistributionsIDs) {
			if (!targetDistribution.equals(sourceDistribution.getID())) {

				double commonTriples = 0.0;

				// iterate over all BF from the main distribution
				for (BloomFilterI mainBF : mainDistributionBFs) {

					for (BloomFilterI partialBF : distributionsBF.get(targetDistribution)) {
						commonTriples = commonTriples + mainBF.intersection(partialBF);
					}
				}
				if (commonTriples > 0.0) {
					returnMap.put(targetDistribution, commonTriples);
				}
			}
		}

		return returnMap;
	}

}
