/**
 * 
 */
package lodVader.tupleManager.processors;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.FileStatement;
import lodVader.utils.FileUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 3, 2016
 */
public class SaveRawDataProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(SaveRawDataProcessor.class);

	DistributionDB distribution;

	FileStatement file = null;
	
	String triplesTmpFilePath;

	/**
	 * Constructor for Class SaveRawDataProcessor
	 */
	public SaveRawDataProcessor(DistributionDB distribution, String fileName) {
		this.distribution = distribution;
		triplesTmpFilePath = LODVaderProperties.BASE_PATH + "/raw_files/" + "__RAW_" + fileName;
		FileUtils.createFolder(LODVaderProperties.BASE_PATH + "/raw_files/");
		file = new FileStatement(LODVaderProperties.BASE_PATH + "/raw_files/", "__RAW_" + fileName);
	}

	public void closeFile() {
		file.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.tupleManager.processors.BasicProcessorInterface#process(org.
	 * openrdf.model.Statement)
	 */
	@Override
	public void process(Statement st) {		
		file.writeStatement(st);
	}
}
