/**
 * 
 */
package org.aksw.idol.plugins.intersection.subset.distribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.aksw.idol.comparator.ComparatorI;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import org.aksw.idol.mongodb.collections.datasetBF.BucketDB;
import org.aksw.idol.plugins.LODVaderPlugin;
import org.aksw.idol.plugins.intersection.LODVaderIntersectionPlugin;
import org.aksw.idol.services.mongodb.BucketService;
import org.aksw.idol.services.mongodb.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class SubsetDetectorBFImpl extends LODVaderIntersectionPlugin {

	public static String PLUGIN_NAME = "SUBSET_BLOOM_FILTER_DETECTOR";

	public static HashMap<String, List<ComparatorI>> bfCache = new HashMap<>();

	public static AtomicBoolean loading = new AtomicBoolean(false);

	private static List<Object> threadList = new ArrayList<Object>();

	/**
	 * Constructor for Class SubsetDetectorBFImpl
	 * 
	 * @param pluginName
	 */
	public SubsetDetectorBFImpl() {
		super(PLUGIN_NAME);
	}

	public HashMap<String, List<ComparatorI>>  loadBucketIntoCache(List<String> distributions) {

		// make all threads hold on for their time
		Object lock = new Object();
		synchronized (lock) {
			if (SubsetDetectorBFImpl.loading.get()) {
				SubsetDetectorBFImpl.threadList.add(this);
				try {
					synchronized (this) {
						this.wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else{
				SubsetDetectorBFImpl.loading.set(true);				
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
		HashMap<String, List<ComparatorI>> queryResult = new BucketService()
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.application.subsetdetection.SubsetDetectionI#detectSubsets()
	 */
//	@Override
	public HashMap<String, Double> runDetection(DistributionDB sourceDistribution,
			List<String> targetDistributionsIDs) {

		HashMap<String, Double> returnMap = new HashMap<String, Double>();
		System.out.println(targetDistributionsIDs.size() + " target distributions found.");

		// load the buckets of the source and the target distriution
//		HashMap<String, List<BloomFilterI>> distributionsBF = getBucketFromDatasets(targetDistributionsIDs);
		HashMap<String, List<ComparatorI>> distributionsBF = loadBucketIntoCache(targetDistributionsIDs);

		// get BF from the source distribution
		List<ComparatorI> mainDistributionBFs = distributionsBF.get(sourceDistribution.getID());

		// compare all bfs
		for (String targetDistribution : targetDistributionsIDs) {
			if (!targetDistribution.equals(sourceDistribution.getID())) {

				double commonTriples = 0.0;

				// iterate over all BF from the main distribution
				for (ComparatorI mainBF : mainDistributionBFs) {

					for (ComparatorI partialBF : distributionsBF.get(targetDistribution)) {
						commonTriples = commonTriples + mainBF.intersection(partialBF);
					}
				}
				if (commonTriples > 100.0) {
					returnMap.put(targetDistribution, commonTriples);
				}
			}
		}

		return returnMap;
	}

}
