/**
 * 
 */
package org.aksw.idol.streaming;

import org.aksw.idol.file.FileStatementCustom;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.services.StatementService;
import org.aksw.idol.tupleManager.PipelineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
public class IDOLFileStream implements IDOLStreamInterface {

	final static Logger logger = LoggerFactory.getLogger(IDOLFileStream.class);

	DistributionDB distribution = null;

	String path = null;
	
	FileStatementCustom fileTriple = null;

	PipelineProcessor pipelineProcessor = new PipelineProcessor();
	StatementService statementUtils = new StatementService();

	/**
	 * Constructor for Class LODVaderRawDataStream
	 */
	public IDOLFileStream(String basePath) {
		this.path = basePath;
	}
	
	public IDOLFileStream() {
	}
	
	public void setPath(String path) {
		this.path = path;
	}

	public PipelineProcessor getPipelineProcessor() {
		return pipelineProcessor;
	}

	public void startParsing(DistributionDB distribution) {
		this.distribution = distribution;

		try {
			logger.info("Loading: " + path + distribution.getID());
			fileTriple = new FileStatementCustom(path+ distribution.getID());
			int triples= 0;
			while(fileTriple.hasNext()){
				pipelineProcessor.handleStatement(fileTriple.getStatement());
				triples++;
			}
			fileTriple.close();
			logger.info(triples+ " triples handled.");			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
