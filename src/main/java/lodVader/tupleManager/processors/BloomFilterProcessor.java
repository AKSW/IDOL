/**
 * 
 */
package lodVader.tupleManager.processors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.utils.FileUtils;
import lodVader.utils.NSUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 3, 2016
 */
public class BloomFilterProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(BloomFilterProcessor.class);

	DistributionDB distribution;

	String triplesTmpFilePath;
	String subjectTmpFilePath;
	String objectTmpFilePath;

	BufferedWriter triplesWriter;
	BufferedWriter subjectWriter;
	BufferedWriter objectWriter;

	enum TYPE_OF_FILE {
		OBJECT, SUBJECT, TRIPLES
	}

	/**
	 * Constructor for Class BloomFilterProcessor
	 */
	public BloomFilterProcessor(DistributionDB distribution) {
		this.distribution = distribution;
		triplesTmpFilePath = LODVaderProperties.TMP_FOLDER + "tmpTriples_" + distribution.getID();
		subjectTmpFilePath = LODVaderProperties.TMP_FOLDER + "tmpSubject_" + distribution.getID();
		objectTmpFilePath = LODVaderProperties.TMP_FOLDER + "tmpObject_" + distribution.getID();
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
		String subject = st.getSubject().toString();
		String object = st.getObject().toString();

		try {
			triplesWriter.write(triple + "\n");
			if (subject.startsWith("http"))
				subjectWriter.write(subject + "\n");
			if (!object.startsWith("\""))
				objectWriter.write(object + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Load a file and save its content to the namespace collections
	 * 
	 * @param file
	 *            the file which contains the list of namespaces
	 * @param generalRosourceNS0
	 *            name of the collection to save namespace level 0
	 * @param generalRosourceNS
	 *            name of the collection to save the mapping between namespace
	 *            (NS0) and dataset/distribution
	 * @param generalRelationNS0
	 *            name of the collection to save namespace
	 * @param generalRelationNS
	 *            name of the collection to save the mapping between namespace
	 *            and dataset/distribution
	 * @return
	 */
	private List<String> saveResources(String file, TYPE_OF_FILE type) {

		logger.info("Saving resources from file " + file);

		List<String> resources = new ArrayList<>();

		HashMap<String, Integer> ns0 = new HashMap<>();
		HashMap<String, Integer> ns = new HashMap<>();

		HashSet<String> bfResources = new HashSet<>();

		int lineCounter = 0;
		int bfCounter = 0;

		try {

			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			NSUtils nsUtils = new NSUtils();
			String lastLine = "";
			int co = 0;
			while ((line = br.readLine()) != null) {

				// ignore repeated triples/resources
				if (!line.equals(lastLine)) {
					lastLine = line;
					lineCounter++;

					if (type != TYPE_OF_FILE.TRIPLES) {
						// extract ns if we are processing objects or subjects
						addToMap(ns, nsUtils.getNSFromString(line));
						addToMap(ns0, nsUtils.getNS0(line));
					}

					bfResources.add(line);

					// save NS into Mongodb each 5k ns
					if (lineCounter % 5000 == 0) {
						if (type == TYPE_OF_FILE.OBJECT) {
							SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0,
									GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS0);
							SaveNS(ns, GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS,
									GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS);
						} else if (type == TYPE_OF_FILE.SUBJECT) {
							SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0,
									GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0);
							SaveNS(ns, GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS,
									GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS);
						}
						ns0 = new HashMap<>();
						ns = new HashMap<>();
					}

					// save Bloom filters each 200k
					if (lineCounter % 200000 == 0) {
						bfCounter++;

						saveBF(bfResources, type, bfCounter);

						bfResources = new HashSet<>();

					}
				}
			}

			// if counter is 200.000 save ns and bloom filters
			if (type == TYPE_OF_FILE.OBJECT) {
				SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0,
						GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS0);
				SaveNS(ns, GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS,
						GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS);
			} else if (type == TYPE_OF_FILE.SUBJECT) {
				SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0,
						GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0);
				SaveNS(ns, GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS,
						GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS);
			}
			if (bfResources.size() > 0)
				saveBF(bfResources, type, bfCounter);

			br.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return resources;
	}

	private void saveBF(HashSet<String> set, TYPE_OF_FILE type, int bfCounter) {


		BloomFilterI bloomFilter = BloomFilterFactory.newBloomFilter();
		bloomFilter.create(200000, 0.0000001);

		for (String str : set) {
			bloomFilter.add(str);
		}

		BucketDB bucket = null;

		if (type == TYPE_OF_FILE.OBJECT) {
			bucket = new BucketDB(BucketDB.COLLECTIONS.BLOOM_FILTER_OBJECTS);
		} else if ((type == TYPE_OF_FILE.SUBJECT)) {
			bucket = new BucketDB(BucketDB.COLLECTIONS.BLOOM_FILTER_SUBJECTS);
		} else {
			bucket = new BucketDB(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES);
		}

		// remove old bloom filters
		bucket.remove(distribution.getID());

		// create the new one

		bucket.saveBF(bloomFilter, distribution.getID(), bfCounter, new ArrayList<String>(set).get(0),
				new ArrayList<String>(set).get(set.size() - 1));

	}

	/**
	 * Save namespaces and their relation with distributions/datasets
	 * 
	 * @param nss
	 * @param resourceCollection
	 * @param relationCollection
	 */
	private void SaveNS(HashMap<String, Integer> nss, GeneralResourceDB.COLLECTIONS resourceCollection,
			GeneralResourceRelationDB.COLLECTIONS relationCollection) {
		List<GeneralResourceDB> resources = new GeneralResourceDB(resourceCollection).insertSet(nss.keySet());

		new GeneralResourceRelationDB(relationCollection).insertSet(nss, resources, distribution.getID(),
				distribution.getTopDatasetID());
	}

	/**
	 * Add value to a map
	 * 
	 * @param map
	 * @param value
	 */
	protected void addToMap(HashMap<String, Integer> map, String value) {
		int n = 0;
		if (map.get(value) != null)
			n = map.get(value);
		map.put(value, n + 1);
	}

	public void saveFilters() {
		closeFiles();

		FileUtils fileUtils = new FileUtils();

		fileUtils.sortFile(objectTmpFilePath);
		fileUtils.sortFile(subjectTmpFilePath);
//		fileUtils.sortFile(triplesTmpFilePath);
		saveResources(objectTmpFilePath, TYPE_OF_FILE.OBJECT);
		fileUtils.removeFile(objectTmpFilePath);
		saveResources(subjectTmpFilePath, TYPE_OF_FILE.SUBJECT);
		fileUtils.removeFile(subjectTmpFilePath);
		saveResources(triplesTmpFilePath, TYPE_OF_FILE.TRIPLES);
		fileUtils.removeFile(triplesTmpFilePath);
		logger.info("BFProcessor finished.");
	}

}
