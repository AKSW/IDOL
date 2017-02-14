/**
 * 
 */
package org.aksw.idol.services.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.aksw.idol.bloomfilters.BloomFilterI;
import org.aksw.idol.mongodb.collections.datasetBF.BucketDB;
import org.aksw.idol.plugins.intersection.subset.distribution.SubsetDetectorBFIntersectImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 28, 2016
 */
public class CachedBucketService  {
	
	final static Logger logger = LoggerFactory.getLogger(CachedBucketService.class);
	
	BucketService bucketService = new BucketService();

	static HashMap<String, BFCache> bfCache = new HashMap<>();

	BucketDB.COLLECTIONS collection;
	
//	public static HashMap<String, List<BloomFilterI>> bfCache = new HashMap<>();

	public static AtomicBoolean loading = new AtomicBoolean(false);

	private static List<Object> threadList = new ArrayList<Object>();

	/**
	 * Constructor for Class CachedBucketService
	 */
	public CachedBucketService(BucketDB.COLLECTIONS collection) {
		this.collection = collection;
	}

	/**
	 * Get a set of bloom filters for a list of distributions
	 * 
	 * @param distributionsIDs
	 *            list of distribution ids
	 * @return a hashmap where the key if the distributionID and the values are
	 *         bloom filters
	 */

	
	public void clear(){
		bfCache = null;
	}
	public HashMap<String, BloomFilterI> loadBucketIntoCache(Collection<String> distributions) {

		// make all threads hold on for their time
		Object lock = new Object();
		synchronized (lock) {
			if (CachedBucketService.loading.get()) {
				CachedBucketService.threadList.add(this);
				try {
					synchronized (this) {
						this.wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else{
				CachedBucketService.loading.set(true);				
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
		HashMap<String, List<BloomFilterI>> queryResult = new BucketService()
				.getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES, distributionsQuery);
		
		logger.info(queryResult.size()+ " filter loaded from database. Total in cache: "+bfCache.size());

		// update cache list
		for (String id : queryResult.keySet()) {
			BFCache c = new BFCache();
			c.bf = queryResult.get(id).iterator().next();
			c.distributionId = id;
			c.ttl = 80;
			bfCache.put(id, c);
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
		
		// return the required filters
		HashMap<String, BloomFilterI> r = new HashMap<>();
		for(String s : distributions){
			r.put(s, bfCache.get(s).bf);
			 bfCache.get(s).ttl++;
		}
		
		// remove filters which were not used over the last 100 requests
		List<String> removeFilters = new ArrayList<>();
		for(String s : bfCache.keySet()){
			 bfCache.get(s).ttl--;
			 if(bfCache.get(s).ttl == 0)
				 removeFilters.add(s);
		}
		for(String s : removeFilters){
			bfCache.remove(s);
		}
		if(removeFilters.size()>0)
			logger.info(removeFilters.size()+ " filters were removed from the cache because they were not longer being used anymore");
		
		
		return r;

	}

	class BFCache {
		int ttl = 10;
		String distributionId;
		BloomFilterI bf;
	}

}
