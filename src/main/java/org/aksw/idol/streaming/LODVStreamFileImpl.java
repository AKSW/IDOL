/**
 * 
 */
package org.aksw.idol.streaming;

import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.tupleManager.PipelineProcessor;
import org.aksw.idol.utils.FileStatement;
import org.aksw.idol.utils.StatementUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
public class LODVStreamFileImpl implements LODVStreamInterface {

	final static Logger logger = LoggerFactory.getLogger(LODVStreamFileImpl.class);

	DistributionDB distribution = null;

	String path = null;
	
	FileStatement fileTriple = null;

	PipelineProcessor pipelineProcessor = new PipelineProcessor();
	StatementUtils statementUtils = new StatementUtils();

	/**
	 * Constructor for Class LODVaderRawDataStream
	 */
	public LODVStreamFileImpl(String basePath) {
		this.path = basePath;
	}

	public PipelineProcessor getPipelineProcessor() {
		return pipelineProcessor;
	}

	public void startParsing(DistributionDB distribution) {
		this.distribution = distribution;

		try {
			logger.info("Loading: " + path + distribution.getID());
			fileTriple = new FileStatement(path+ distribution.getID());
			int triples= 0;
			while(fileTriple.hasNext()){
				pipelineProcessor.handleStatement(fileTriple.getStatement());
				triples++;
			}
			fileTriple.close();
			logger.info(triples+ " triples handled.");			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
