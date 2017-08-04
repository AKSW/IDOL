package org.aksw.idol.bloomfilters.impl;

import org.aksw.idol.bloomfilters.BloomFilterI;
import org.aksw.idol.streaming.IDOLFileStream;
import org.junit.Test;

public class GoogleBloomFilterTest {
	
	BloomFilterI bf = BloomFilterFactory.newBloomFilter();
	
	@Test
	public void testPrecision(){
		IDOLFileStream stream = new IDOLFileStream("");
	}

}
