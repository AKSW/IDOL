/**
 * 
 */
package org.aksw.idol.core.streaming;

import org.aksw.idol.core.mongodb.collections.DistributionDB;
import org.aksw.idol.core.tupleManager.PipelineProcessor;

/**
 * @author Ciro Baron Neto
 * 
 * Nov 14, 2016
 */
public interface LODVStreamInterface {
	
	
	/**
	 * Get an instance of pipeline processor. Classes that implements LODVStreamInterface should have one pipeline processor.
	 * @return the pipeline processor
	 */
	public PipelineProcessor getPipelineProcessor();
	
	
	/**
	 * Start parsing a distributions
	 * @param distribution
	 */
	public void startParsing(DistributionDB distribution) throws Exception;
	

}
