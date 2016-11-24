/**
 * 
 */
package lodVader.tupleManager.processors;

import java.io.File;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.FileStatement;
import lodVader.utils.FileUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 3, 2016
 */
public class SaveDumpDataProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(SaveDumpDataProcessor.class);

	DistributionDB distribution;

	FileStatement file = null;
	
	String triplesTmpFilePath;
	
	int triples = 0;

	/**
	 * Constructor for Class SaveRawDataProcessor
	 */
	public SaveDumpDataProcessor(DistributionDB distribution, String fileName) {
		this.distribution = distribution;
		triplesTmpFilePath = LODVaderProperties.BASE_PATH + "/raw_files/" + "__RAW_" + fileName;
		FileUtils.createFolder(LODVaderProperties.BASE_PATH + "/raw_files/");
		new File(LODVaderProperties.BASE_PATH + "/raw_files/", "__RAW_" + fileName).delete();
		file = new FileStatement(LODVaderProperties.BASE_PATH + "/raw_files/", "__RAW_" + fileName);
	}

	public void closeFile() {
		file.close();
		distribution.setNumberOfTriples(triples);
		try {
			distribution.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		triples++;
	}
}
