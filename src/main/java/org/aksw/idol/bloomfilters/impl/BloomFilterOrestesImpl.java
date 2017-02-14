/**
 * 
 */
package org.aksw.idol.bloomfilters.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.aksw.idol.bloomfilters.BloomFilterI;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 4, 2016
 */
public class BloomFilterOrestesImpl implements BloomFilterI {

	public BloomFilter<String> bf = null;

	int numberOfElements = 0;

	int initialSize = 0;

	Double fpp;

	/*
	 * (non-Javadoc)
	 * 
	 * @see idol.bloomfilters.BloomFilterI#create(int, double)
	 */
	@Override
	public boolean create(int initialSize, double fpp) {

		if (fpp > 1)
			fpp = 0.000_000_1;
		if (initialSize < 500)
			initialSize = 500;
		
		//murmu
		// carter

		if (bf == null)
			bf = new FilterBuilder(initialSize, fpp)
//					bf = new FilterBuilder()
//					.expectedElements(initialSize)
//					.size(820000*8)
//					.hashes(1)
////					.hashFunction(HashMethod.Murmur3)
//					.hashes(2).hashFunction(HashMethod.CarterWegman)
//					.hashes(3).hashFunction(HashMethod.Murmur3KirschMitzenmacher)
//					.hashes(4).hashFunction(HashMethod.MD5)
//					.hashes(5).hashFunction(HashMethod.MD2)
//					.hashes(6).hashFunction(HashMethod.Murmur2)
			.buildBloomFilter();
		this.initialSize = initialSize;
		this.fpp = fpp;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#add(java.lang.String)
	 */
	@Override
	public boolean add(String element) {
		bf.add(element);
		numberOfElements++;

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#compare(java.lang.String)
	 */
	@Override
	public boolean compare(String element) {
		return bf.contains(element);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#getNumberOfElements()
	 */
	@Override
	public int getNumberOfElements() {
		return numberOfElements;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#readFrom(java.io.InputStream)
	 */
	@Override
	public void readFrom(InputStream in) throws IOException {
		ObjectInputStream restore = new ObjectInputStream(in);
		try {
			bf = (BloomFilter<String>) restore.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		restore.close();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#writeTo(java.io.OutputStream)
	 */
	@Override
	public void writeTo(OutputStream out) throws IOException {
		ObjectOutputStream bfOut = new ObjectOutputStream(out);
		bfOut.writeObject(bf);
		bfOut.close();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#getFPP()
	 */
	@Override
	public double getFPP() {
		return this.fpp;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.bloomfilters.BloomFilterI#getFilterInitialSize()
	 */
	@Override
	public double getFilterInitialSize() {
		return initialSize;
	}
	
	public boolean intersect(BloomFilterOrestesImpl bloomFilter){
		return bf.intersect(bloomFilter.bf);
	}
	

	/* (non-Javadoc)
	 * @see lodVader.bloomfilters.BloomFilterI#intersection(lodVader.bloomfilters.BloomFilterI)
	 */
	@Override
	public Double intersection(BloomFilterI toIntersectWith) {
		
		BloomFilter<String> newBF = bf.clone();
		
		if (newBF.intersect((BloomFilter<String>) toIntersectWith.getImplementation()))
			return Math.floor(newBF.getEstimatedPopulation());		
		return null;
	}

	/* (non-Javadoc)
	 * @see lodVader.bloomfilters.BloomFilterI#getImplementation()
	 */
	@Override
	public Object getImplementation() {
		return bf;
	}

}
