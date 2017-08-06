package org.aksw.idol.bloomfilters.impl;

import org.aksw.idol.comparator.ComparatorI;
import org.aksw.idol.comparator.bloomfilters.impl.ComparatorFactory;
import org.aksw.idol.streaming.IDOLFileStream;
import org.junit.Test;


public class GoogleBloomFilterTest {
	
	ComparatorI bf = ComparatorFactory.newComparator();
	
	@Test
	public void testPrecision(){
		IDOLFileStream stream = new IDOLFileStream("");
	}

}
