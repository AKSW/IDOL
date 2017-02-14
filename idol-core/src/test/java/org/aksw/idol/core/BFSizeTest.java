package org.aksw.idol.core;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.aksw.idol.core.bloomfilters.BloomFilterI;
import org.aksw.idol.core.bloomfilters.impl.BloomFilterFactory;
import org.aksw.idol.utils.Timer;
import org.junit.Test;

/**
 * 
 */

/**
 * @author Ciro Baron Neto
 * 
 * Nov 23, 2016
 */
public class BFSizeTest {
	
	// time 1063
	@Test
	public void bfSizeTest(){
		BloomFilterI bf = BloomFilterFactory.newBloomFilter();
		DecimalFormat formatter = new DecimalFormat("#,###,###,###,###,###");
		bf.create(92_000_000, 0.00001);
		
		int size = 90_000_000;
		
		Timer t1 = new Timer(); 
		int i = 0;
		t1.startTimer();
		while(i++<size){
			bf.add("yada" + i);
			if(i%10_000_000==0)
				System.out.println("Creating: "+formatter.format(i));
		}
		bf.add("XXxX");
		
		try {
			bf.writeTo(new FileOutputStream(new File("/tmp/filter")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		int fp = 0;
		
		i = 0;
		
		while(i++<size){
			if(bf.compare("Yoda"+1))
				fp++;
			if(i%10_000_000==0){
				System.out.println("Comparing: "+ formatter.format(i));
				System.out.println("fp: "+ formatter.format(fp));
				
			} 

		}
		
		if(bf.compare("XXxX"))
			fp++;
		
		System.out.println(fp);
		System.out.println(t1.stopTimer());
		
		
	}

}
