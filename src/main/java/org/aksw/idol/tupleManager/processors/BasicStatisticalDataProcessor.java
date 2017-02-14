/**
 * 
 */
package org.aksw.idol.tupleManager.processors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.aksw.idol.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceRelationDB;
import org.aksw.idol.utils.FileUtils;
import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Class which extracts basic statistical data from the dataset (e.g.
 * predicates, owlClass, etc...)
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 2, 2016
 */
public class BasicStatisticalDataProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(BasicStatisticalDataProcessor.class);

	DistributionDB distribution;

	/**
	 * Constructor for Class BasicStatisticalDataProcessor
	 */
	public BasicStatisticalDataProcessor(DistributionDB distribution) {
		this.distribution = distribution;
//		allPredicatesFileName = LODVaderProperties.TMP_FOLDER + "tmpPredicates_" + distribution.getID();
//		rdfTypeObjectsFileName = LODVaderProperties.TMP_FOLDER + "tmpRdfTypeObjects_" + distribution.getID();
//		owlClassesFileName = LODVaderProperties.TMP_FOLDER + "tmpOwlClasses_" + distribution.getID();
//		rdfSubClassOfFileName = LODVaderProperties.TMP_FOLDER + "tmpRdfSubClassOf_" + distribution.getID();
//		initializeWriters();
	}

	// total number of triples
	Integer numberOfTriples = 0;

	// number of literals
	Integer numberOfLiterals = 0;
	
	// total number of blank nodes
	Integer numberOfBlankNodes= 0;

	
	// files
	public String allPredicatesFileName;
	public BufferedWriter allPredicatesWriter;

	public String rdfTypeObjectsFileName;
	public BufferedWriter rdfTypeObjectsWriter;

	public String owlClassesFileName;
	public BufferedWriter owlClassesWriter;

	public String rdfSubClassOfFileName;
	public BufferedWriter rdfSubClassOfWriter;

	private void initializeWriters() {
		try {
			allPredicatesWriter = new BufferedWriter(new FileWriter(new File(allPredicatesFileName)));
			rdfTypeObjectsWriter = new BufferedWriter(new FileWriter(new File(rdfTypeObjectsFileName)));
			owlClassesWriter = new BufferedWriter(new FileWriter(new File(owlClassesFileName)));
			rdfSubClassOfWriter = new BufferedWriter(new FileWriter(new File(rdfSubClassOfFileName)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void closeWriters() {
		try {
			allPredicatesWriter.close();
			rdfSubClassOfWriter.close();
			owlClassesWriter.close();
			rdfSubClassOfWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
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

	protected void writeToFile(String resource, BufferedWriter writer) {
		try {
			writer.write(resource + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * idol.tupleManager.statisticalDataProcessors.BasicProcessorInterface#
	 * processStatisticalData(org.openrdf.model.Statement)
	 */
	@Override
	public void process(Statement st) {
		
		// collects all predicates
		// addToMap(allPredicates, st.getPredicate().stringValue());
//		writeToFile(st.getPredicate().stringValue(), allPredicatesWriter);

		numberOfTriples++;

		// collects owl:class, subclass and rdftype.
		if (st.getSubject().stringValue().startsWith("http")) {
			if (st.getObject().stringValue().equals("http://www.w3.org/2002/07/owl#Class")) {
				// addToMap(owlClasses, st.getSubject().stringValue());
//				writeToFile(st.getSubject().stringValue(), owlClassesWriter);
			}

		} else
			numberOfBlankNodes++;
		
		if (!st.getObject().stringValue().startsWith("http") && !st.getObject().stringValue().startsWith("_:")) {	
			numberOfLiterals ++;
		}

//		if (st.getPredicate().stringValue().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
//			// addToMap(rdfSubClassOf, st.getObject().stringValue());
//			writeToFile(st.getObject().stringValue(), rdfSubClassOfWriter);
//
//		} else if (st.getPredicate().stringValue().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
//			// addToMap(rdfTypeObjects, st.getObject().stringValue());
//			writeToFile(st.getObject().stringValue(), rdfTypeObjectsWriter);
//		}
	}

	private void saveResources(String file, GeneralResourceDB.COLLECTIONS resourceCollection,
			GeneralResourceRelationDB.COLLECTIONS relationCollection) {

		HashMap<String, Integer> resources = new HashMap<String, Integer>();

		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(file)));

			String line;
			String lastLine = "";

			while ((line = br.readLine()) != null) {

				lastLine = line;

				if (resources.size() % 5000 == 0) {
					// remove the last element (because will have the counter ==
					// 1, and the counter is bigger if the current line is
					// equals the last one
					if (lastLine.equals(line)) {
						resources.remove(line);
					}
					List<GeneralResourceDB> generalResources = new GeneralResourceDB(resourceCollection)
							.insertSet(resources.keySet());

					new GeneralResourceRelationDB(relationCollection).insertSet(resources, generalResources, distribution.getID()
							);

					resources = new HashMap<String, Integer>();

					// add the last resource into the set
					if (lastLine.equals(line)) {
						addToMap(resources, line);
					}

				}

				addToMap(resources, line);
			}
			
			// save the rest 
			List<GeneralResourceDB> generalResources = new GeneralResourceDB(resourceCollection)
					.insertSet(resources.keySet());

			new GeneralResourceRelationDB(relationCollection).insertSet(resources, generalResources, distribution.getID());
			
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void saveStatisticalData() {
//		FileUtils fileUtils = new FileUtils();
//
//		closeWriters();
//		
//		fileUtils.sortFile(allPredicatesFileName);
//		fileUtils.sortFile(owlClassesFileName);
//		fileUtils.sortFile(rdfSubClassOfFileName);
//		fileUtils.sortFile(rdfTypeObjectsFileName);
//
//		logger.info("Saving predicates...");
//		saveResources(allPredicatesFileName, GeneralResourceDB.COLLECTIONS.RESOURCES_ALL_PREDICATES, GeneralResourceRelationDB.COLLECTIONS.RELATION_ALL_PREDICATES);
//		fileUtils.removeFile(allPredicatesFileName);
//		
//		logger.info("Saving rdf:type objects...");
//		saveResources(rdfTypeObjectsFileName, GeneralResourceDB.COLLECTIONS.RESOURCES_RDF_TYPE, GeneralResourceRelationDB.COLLECTIONS.RELATION_RDF_TYPE);
//		fileUtils.removeFile(rdfTypeObjectsFileName);
//				
//		logger.info("Saving rdfs:subclass objects...");
//		saveResources(rdfSubClassOfFileName, GeneralResourceDB.COLLECTIONS.RESOURCES_RDF_SUBCLASS, GeneralResourceRelationDB.COLLECTIONS.RELATION_RDF_SUBCLASS);
//		fileUtils.removeFile(rdfSubClassOfFileName);
//				
//		logger.info("Saving owl:Class objects...");
//		saveResources(owlClassesFileName, GeneralResourceDB.COLLECTIONS.RESOURCES_OWL_CLASS, GeneralResourceRelationDB.COLLECTIONS.RELATION_OWL_CLASS);
//		fileUtils.removeFile(owlClassesFileName);
				
		distribution.setNumberOfLiterals(numberOfLiterals);
//		distribution.setNumberOfTriples(numberOfTriples);
		distribution.setNumberOfBlankNodes(numberOfBlankNodes);

		try {
			distribution.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
