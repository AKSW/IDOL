/**
 * 
 */
package lodVader.streaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.tupleManager.PipelineProcessor;
import lodVader.utils.FileStatement;
import lodVader.utils.StatementUtils;

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
			fileTriple = new FileStatement(path, "__RAW_" + distribution.getID());
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
