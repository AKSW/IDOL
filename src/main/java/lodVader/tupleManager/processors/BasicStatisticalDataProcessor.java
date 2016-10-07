/**
 * 
 */
package lodVader.tupleManager.processors;

import java.util.HashMap;
import java.util.List;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.GeneralResourceDB;
import lodVader.mongodb.collections.RDFResources.GeneralResourceRelationDB;

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
	}

	// total number of triples
	Integer numberOfTriples = 0;

	// number of literals
	Integer numberOfLiterals = 0;

	// map with all predicates and their frequency
	public HashMap<String, Integer> allPredicates = new HashMap<String, Integer>();

	// saving all rdf type
	public HashMap<String, Integer> rdfTypeObjects = new HashMap<String, Integer>();

	// all classes
	public HashMap<String, Integer> owlClasses = new HashMap<String, Integer>();

	// and all subclasses
	public HashMap<String, Integer> rdfSubClassOf = new HashMap<String, Integer>();

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.tupleManager.statisticalDataProcessors.BasicProcessorInterface#
	 * processStatisticalData(org.openrdf.model.Statement)
	 */
	@Override
	public void process(Statement st) {

		// collects all predicates
		addToMap(allPredicates, st.getPredicate().toString());

		numberOfTriples++;

		// collects owl:class, subclass and rdftype.
		if (st.getObject().toString().startsWith("http")) {
			if (st.getObject().toString().equals("http://www.w3.org/2002/07/owl#Class")) {
				addToMap(owlClasses, st.getObject().toString());
			}
		} else
			numberOfLiterals++;

		if (st.getPredicate().toString().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
			addToMap(rdfSubClassOf, st.getPredicate().toString());
		} else if (st.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
			addToMap(rdfTypeObjects, st.getObject().toString());
		}
	}

	public void saveStatisticalData() {

		logger.info("Saving predicates...");
		List<GeneralResourceDB> resources = new GeneralResourceDB(
				GeneralResourceDB.COLLECTIONS.RESOURCES_ALL_PREDICATES).insertSet(allPredicates.keySet());

		new GeneralResourceRelationDB(GeneralResourceRelationDB.COLLECTIONS.RELATION_ALL_PREDICATES)
				.insertSet(allPredicates, resources, distribution.getID(), distribution.getTopDatasetID());

		logger.info("Saving rdf:type objects...");
		resources = new GeneralResourceDB(GeneralResourceDB.COLLECTIONS.RESOURCES_RDF_TYPE)
				.insertSet(rdfTypeObjects.keySet());

		new GeneralResourceRelationDB(GeneralResourceRelationDB.COLLECTIONS.RELATION_RDF_TYPE).insertSet(rdfTypeObjects,
				resources, distribution.getID(), distribution.getTopDatasetID());

		logger.info("Saving rdfs:subclass objects...");
		resources = new GeneralResourceDB(GeneralResourceDB.COLLECTIONS.RESOURCES_RDF_SUBCLASS)
				.insertSet(rdfSubClassOf.keySet());

		new GeneralResourceRelationDB(GeneralResourceRelationDB.COLLECTIONS.RELATION_RDF_SUBCLASS)
				.insertSet(rdfSubClassOf, resources, distribution.getID(), distribution.getTopDatasetID());

		logger.info("Saving owl:Class objects...");
		resources = new GeneralResourceDB(GeneralResourceDB.COLLECTIONS.RESOURCES_OWL_CLASS)
				.insertSet(owlClasses.keySet());

		new GeneralResourceRelationDB(GeneralResourceRelationDB.COLLECTIONS.RELATION_OWL_CLASS).insertSet(owlClasses,
				resources, distribution.getID(), distribution.getTopDatasetID());

		distribution.setNumberOfLiterals(numberOfLiterals);
		distribution.setNumberOfTriples(numberOfTriples);

		try {
			distribution.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
