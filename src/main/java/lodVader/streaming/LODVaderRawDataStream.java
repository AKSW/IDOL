/**
 * 
 */
package lodVader.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.management.StandardEmitterMBean;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.application.LODVader;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.tupleManager.PipelineProcessor;
import lodVader.utils.FileStatement;
import lodVader.utils.StatementUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
public class LODVaderRawDataStream {

	final static Logger logger = LoggerFactory.getLogger(LODVaderRawDataStream.class);

	DistributionDB distribution = null;

	String path = null;
	
	FileStatement fileTriple = null;

	PipelineProcessor pipelineProcessor = new PipelineProcessor();
	StatementUtils statementUtils = new StatementUtils();

	/**
	 * Constructor for Class LODVaderRawDataStream
	 */
	public LODVaderRawDataStream(String basePath) {
		this.path = basePath;
	}

	public PipelineProcessor getPipelineProcessor() {
		return pipelineProcessor;
	}

	public void startParsing(DistributionDB distribution) {
		this.distribution = distribution;

		try {
			logger.info("Loading: " + path + distribution.getID());
			fileTriple = new FileStatement(path, "__RAW_" + distribution.getID());
			while(fileTriple.hasNext())
				pipelineProcessor.handleStatement(fileTriple.getStatement());
			fileTriple.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
