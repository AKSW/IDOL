/**
 * 
 */
package lodVader.tupleManager.processors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 3, 2016
 */
public class SaveRawDataProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(SaveRawDataProcessor.class);

	DistributionDB distribution;

	String triplesTmpFilePath;
	BufferedWriter triplesWriter;

	/**
	 * Constructor for Class SaveRawDataProcessor
	 */
	public SaveRawDataProcessor(DistributionDB distribution, String fileName) {
		this.distribution = distribution;
		triplesTmpFilePath = LODVaderProperties.TMP_FOLDER +fileName;
		openFiles();
	}

	public void openFiles() {
		try {
			triplesWriter = new BufferedWriter(new FileWriter(new File(triplesTmpFilePath)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeFiles() {
		try {
			triplesWriter.close();
		} catch (IOException e) {
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

		String triple = st.getSubject().toString() + " " + st.getPredicate() + " " + st.getObject();

//		System.out.println(triple);
		
		
		try {
			triplesWriter.write(triple + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void saveFile() {
		closeFiles();
	}

}
