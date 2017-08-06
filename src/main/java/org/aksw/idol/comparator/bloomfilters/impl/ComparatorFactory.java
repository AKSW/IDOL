/**
 * 
 */
package org.aksw.idol.comparator.bloomfilters.impl;

import org.aksw.idol.comparator.ComparatorI;

/**
 * @author Ciro Baron Neto
 * 
 * Jul 7, 2016
 */
public class ComparatorFactory {
	
	public static ComparatorI newComparator(){ 
		return new HashSetImpl();
//		return new BloomFilterGoogleImpl();
//		return new BloomFilterOrestesImpl();
	}

}
