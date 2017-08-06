package org.aksw.idol.comparator.bloomfilters.impl;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;

import org.aksw.idol.comparator.ComparatorI;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

/**
 * @author Ciro Baron Neto
 * 
 *         Implementation of Bloom Filter interface using Google Guava
 * 
 *         Jul 4, 2014
 */
class HashSetImpl implements ComparatorI {

	private int insertions = 0;

	private long initialSize = 0;
	
	HashSet<String> set;
	public boolean create(long initialSize, double fpp) {
		this.set = new HashSet<>();
		return true;
	}

	public boolean add(String element) {
		set.add(element);
		return true;
	}

	public boolean compare(String element) {
		return set.contains(element);
	}


	public long getNumberOfElements() {
		return set.size();
	}


	public double getFPP() {
		return 0;
	}


	public double getFilterInitialSize() {
		return initialSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see bloomfilter.BloomFilterI#readFrom(java.io.InputStream)
	 */
	public void readFrom(InputStream in) throws IOException {
//		filter = BloomFilter.readFrom(in, funnel);
	}


	public void writeTo(OutputStream out) throws IOException {
//		filter.writeTo(new BufferedOutputStream(out));
	}


	@Override
	public Double intersection(ComparatorI toIntersectWith) {
		return null;
	}


	@Override
	public Object getImplementation() {
		return set;
	}

}
