/**
 * 
 */
package lodVader.tupleManager.processors;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.externalsorting.ExternalSort;

import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.bloomfilter.BloomFilterCache;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 3, 2016
 */
public class BloomFilterProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(BloomFilterProcessor.class);

	public BloomFilterCache subjectFilters = new BloomFilterCache(200000, 0.0000001);
	public BloomFilterCache objectFilters = new BloomFilterCache(200000, 0.0000001);
	public BloomFilterCache triplesFilter = new BloomFilterCache(200000, 0.0000001);

	DistributionDB distribution;

	String triplesTmpFilePath;
	String subjectTmpFilePath;
	String objectTmpFilePath;

	BufferedWriter triplesWriter;
	BufferedWriter subjectWriter;
	BufferedWriter objectWriter;

	/**
	 * Constructor for Class BloomFilterProcessor
	 */
	public BloomFilterProcessor(DistributionDB distribution) {
		this.distribution = distribution;
		triplesTmpFilePath = LODVaderProperties.TMP_FOLDER+ "/tmpTriples_" + distribution.getID();
		subjectTmpFilePath = LODVaderProperties.TMP_FOLDER + "/tmpSubject_" + distribution.getID();
		objectTmpFilePath = LODVaderProperties.TMP_FOLDER + "/tmpObject_" + distribution.getID();
		openFiles();
	}

	public void openFiles() {
		try {
			triplesWriter = new BufferedWriter(new FileWriter(new File(triplesTmpFilePath)));
			subjectWriter = new BufferedWriter(new FileWriter(new File(subjectTmpFilePath)));
			objectWriter = new BufferedWriter(new FileWriter(new File(objectTmpFilePath)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeFiles() {
		try {
			triplesWriter.close();
			objectWriter.close();
			subjectWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void removeFile(String file){
		new File(file).delete();
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

		String triple = st.getSubject().toString() +" "+ st.getPredicate() +" "+ st.getObject();
		String subject = st.getSubject().toString();
		String object = st.getObject().toString();

		try {
			triplesWriter.write(triple+"\n");
			subjectWriter.write(subject+"\n");
			if(!object.startsWith("\""))
				objectWriter.write(object+"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void saveFilters() {
		
		closeFiles();
		
		//sort file
		try {
			ExternalSort.sort(new File(objectTmpFilePath), new File(objectTmpFilePath+".sorted"));
			removeFile(objectTmpFilePath);
			ExternalSort.sort(new File(subjectTmpFilePath), new File(subjectTmpFilePath+".sorted"));
			removeFile(subjectTmpFilePath);
			ExternalSort.sort(new File(triplesTmpFilePath), new File(triplesTmpFilePath+".sorted"));
			removeFile(triplesTmpFilePath);
			
			logger.info("Files sorted.");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		closeFiles();
	}

}
