/**
 * 
 */
package org.aksw.idol.core.utils;

import java.util.ArrayList;

import org.aksw.idol.core.bloomfilters.BloomFilterI;
import org.aksw.idol.core.bloomfilters.impl.BloomFilterFactory;
import org.aksw.idol.core.parsers.descriptionFileParser.DescriptionFileParserLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Jul 7, 2016
 */
public class BloomFilterCache {

	final static Logger logger = LoggerFactory.getLogger(BloomFilterCache.class);

	// default bloom filter size
	private int initialSize;

	// default bloom filter fpp
	private double fpp;

	private ArrayList<BloomFilterI> caches = new ArrayList<BloomFilterI>();

	public BloomFilterCache(int initialSize, double bfFpp) {
		this.initialSize = initialSize;
		this.fpp = bfFpp;
		BloomFilterI bf = BloomFilterFactory.newBloomFilter();
		bf.create(initialSize, bfFpp);
		caches.add(bf);
		logger.info("New BF created! ");

	}

	/**
	 * Query
	 * 
	 * @param query
	 * @return true case the query element was found
	 */
	public boolean compare(String query) {
		for (BloomFilterI cache : caches) {
			if (cache.compare(query)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Add a new element
	 * 
	 * @param resource
	 * @param datasetID
	 */
	public void add(String resource) {
		// do not overload the BFs with a value higher than bfSize
		for (BloomFilterI cache : caches) {
			if (cache.getNumberOfElements() < initialSize) {
				cache.add(resource);
				return;
			}
		}

		// case all caches are full, create a new one and add the
		// list
		try {
			BloomFilterI cache = BloomFilterFactory.newBloomFilter();
			cache.create(initialSize, fpp);
			cache.add(resource);
			caches.add(cache);
			logger.info("New BF created! ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sometimes JAVA GC takes time to empty the list of BF. Since this class is
	 * quite memory consuming, this method will empty all list of BFs
	 */
	public void empty() {
		caches = null;
	}

}
