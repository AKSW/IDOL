/**
 * 
 */
package lodVader.utils.bloomfilter;

import java.util.ArrayList;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;

/**
 * @author Ciro Baron Neto
 * 
 * Jul 7, 2016
 */
public class BloomFilterCache {
	

	// default bloom filter size
	private int initialSize = 200000;
	
	// maximun number of elements per BF (notice this is different of initialSize, since it controls the number of added elements)
	private int threshold = initialSize/2;
	
	private int numberOfElements = 0;

	// default bloom filter fpp
	private double fpp = 0.000001;

	private ArrayList<BloomFilterI> caches = new ArrayList<BloomFilterI>();

	
	public BloomFilterCache(int initialSize, double bfFpp) {
		this.initialSize = initialSize;
		this.fpp = bfFpp;
		BloomFilterI bf = BloomFilterFactory.newBloomFilter();  
		bf.create(initialSize, bfFpp);
		caches.add(bf);
	}
	
	public BloomFilterCache(int initialSize, double bfFpp, BloomFilterI bf) {
		this.initialSize = initialSize;
		this.fpp = bfFpp;
		caches.add(bf);
	}

	/**
	 * Query 
	 * 
	 * @param query
	 * @return true case the query element was found
	 */
	public boolean contain(String query) {
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
			
			// if element has already been inserted 
			if(cache.compare(resource))
				return;
			
			else if (cache.getNumberOfElements() < threshold) {
				cache.add(resource);
				numberOfElements++;
				return;
			}
		}

		// case all caches are full, create a new one and add the
		// list
		BloomFilterI cache = BloomFilterFactory.newBloomFilter(); 
		cache.create(initialSize, fpp);
		cache.add(resource);
		caches.add(cache); 
	}

	/**
	 * Sometimes JAVA GC takes time to empty the list of BF. Since this class is
	 * quite memory consuming, this method will empty all list of BFs
	 */
	public void empty() {
		caches = null;
	}
	
	public int numberOfElements(){
		return numberOfElements;
	}
	
	/**
	 * @return the caches
	 */
	public ArrayList<BloomFilterI> getListOfBF() {
		return caches;
	}
	
	/**
	 * @return the initialSize
	 */
	public int getInitialSize() {
		return initialSize;
	}
	
	/**
	 * @return the fpp
	 */
	public double getFpp() {
		return fpp;
	}
	
}
