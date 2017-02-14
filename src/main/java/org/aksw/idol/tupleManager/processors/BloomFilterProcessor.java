/**
 * 
 */
package org.aksw.idol.tupleManager.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.aksw.idol.bloomfilters.BloomFilterI;
import org.aksw.idol.bloomfilters.impl.BloomFilterFactory;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceRelationDB;
import org.aksw.idol.mongodb.collections.datasetBF.BucketDB;
import org.aksw.idol.services.mongodb.BucketService;
import org.aksw.idol.utils.FileList;
import org.aksw.idol.utils.NSUtils;
import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 3, 2016
 */
public class BloomFilterProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(BloomFilterProcessor.class);

	DistributionDB distribution;

	FileList<String> triplesWriter;
	FileList<String> subjectWriter;
	FileList<String> objectWriter;

	enum TYPE_OF_FILE {
		OBJECT, SUBJECT, TRIPLES
	}

	/**
	 * Constructor for Class BloomFilterProcessor
	 */
	public BloomFilterProcessor(DistributionDB distribution) {
		this.distribution = distribution;
		triplesWriter = new FileList<>(LODVaderProperties.TMP_FOLDER, "tmpTriples_" + distribution.getID());
		subjectWriter = new FileList<>(LODVaderProperties.TMP_FOLDER, "tmpSubject_" + distribution.getID());
		objectWriter = new FileList<>(LODVaderProperties.TMP_FOLDER, "tmpObject_" + distribution.getID());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * idol.tupleManager.processors.BasicProcessorInterface#process(org.
	 * openrdf.model.Statement)
	 */
	@Override
	public void process(Statement st) {

		String triple = st.getSubject().stringValue() + " " + st.getPredicate().stringValue() + " "
				+ st.getObject().stringValue();
		String subject = st.getSubject().stringValue();
		String object = st.getObject().stringValue();

		triplesWriter.add(triple);

		if (subject.startsWith("http"))
			subjectWriter.add(subject);
		if (!object.startsWith("\"") && !object.startsWith("\"_"))
			objectWriter.add(object);

	}

	/**
	 * Load a file and save its content to the namespace collections
	 * 
	 * @param list
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
	private List<String> saveResources(FileList<String> list, TYPE_OF_FILE type) {

		logger.info("Saving resources from file " + list.getFullPath());

		List<String> resources = new ArrayList<>();

		/**
		 * Collections which will hold the namespaces
		 */
		HashMap<String, Integer> ns0 = new HashMap<>();
		// HashMap<String, Integer> ns = new HashMap<>();

		/**
		 * Choosing the right bucket collection
		 */
		BucketDB.COLLECTIONS collection;

		if (type == TYPE_OF_FILE.OBJECT) {
			collection = BucketDB.COLLECTIONS.BLOOM_FILTER_OBJECTS;
		} else if ((type == TYPE_OF_FILE.SUBJECT)) {
			collection = BucketDB.COLLECTIONS.BLOOM_FILTER_SUBJECTS;
		} else {
			collection = BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES;
		}

		/**
		 * Creating a bloom filter
		 */
		BloomFilterI bloomFilter = BloomFilterFactory.newBloomFilter();
		bloomFilter.create(list.size(), 0.000_01);

		String line;
		NSUtils nsUtils = new NSUtils();
		String lastLine = "";

		while (list.hasNext()) {
			line = list.next();

			if (type != TYPE_OF_FILE.TRIPLES) {
				// extract ns if we are processing objects or subjects
				// addToMap(ns, nsUtils.getNSFromString(line));
				addToMap(ns0, nsUtils.getNS0(line));
			}

			// ignore repeated triples/resources
			if (!line.equals(lastLine)) {
				lastLine = line;

				bloomFilter.add(line);

				// save NS into Mongodb each 5k ns
				if (ns0.size() % 5000 == 0) {
					if (type == TYPE_OF_FILE.OBJECT) {
						SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0,
								GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS0);
						// SaveNS(ns,
						// GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS,
						// GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS);
					} else if (type == TYPE_OF_FILE.SUBJECT) {
						SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0,
								GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0);
						// SaveNS(ns,
						// GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS,
						// GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS);
					}
					ns0 = new HashMap<>();
					// ns = new HashMap<>();
				}
			}
		}

		// if counter is 200.000 save ns and bloom filters
		if (type == TYPE_OF_FILE.OBJECT) {
			SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0,
					GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS0);
			// SaveNS(ns, GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS,
			// GeneralResourceRelationDB.COLLECTIONS.RELATION_OBJECT_NS);
		} else if (type == TYPE_OF_FILE.SUBJECT) {
			SaveNS(ns0, GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0,
					GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS0);
			// SaveNS(ns, GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS,
			// GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS);
		}

		/**
		 * Saving buckets
		 */
		if (type == TYPE_OF_FILE.SUBJECT || type == TYPE_OF_FILE.TRIPLES) {
			BucketDB bucket = new BucketDB(collection, bloomFilter, distribution.getID(), 0, null, null);
			new BucketService().removeBucket(collection, distribution.getID());
			new BucketService().saveBucket(bucket);
		}
		list.close();

		return resources;
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

		new GeneralResourceRelationDB(relationCollection).insertSet(nss, resources, distribution.getID());
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
		objectWriter.close();
		subjectWriter.close();
		triplesWriter.close();

		saveResources(objectWriter, TYPE_OF_FILE.OBJECT);
		saveResources(subjectWriter, TYPE_OF_FILE.SUBJECT);
		saveResources(triplesWriter, TYPE_OF_FILE.TRIPLES);

		objectWriter.clear();
		subjectWriter.clear();
		triplesWriter.clear();

		logger.info(getClass().getName() + " finished.");
	}

}
