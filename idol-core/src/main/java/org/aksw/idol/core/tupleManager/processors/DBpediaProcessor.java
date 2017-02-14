/**
 * 
 */
package org.aksw.idol.core.tupleManager.processors;

import org.aksw.idol.core.mongodb.collections.DistributionDB;
import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which extracts basic statistical data from the dataset (e.g.
 * predicates, owlClass, etc...)
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 2, 2016
 */
public class DBpediaProcessor implements BasicProcessorInterface {

	final static Logger logger = LoggerFactory.getLogger(DBpediaProcessor.class);

	DistributionDB distribution; 

	/**
	 * Constructor for Class BasicStatisticalDataProcessor
	 */
	public DBpediaProcessor(DistributionDB distribution) {
		this.distribution = distribution;
	}

	// total number of triples
	public Integer numberOfTriples = 0;

	public Integer numberOfDbpediaObjects = 0;
	public Integer numberOfDbpediaProperties = 0;
	public Integer numberOfDbpediaSubject = 0;
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * idol.tupleManager.statisticalDataProcessors.BasicProcessorInterface#
	 * processStatisticalData(org.openrdf.model.Statement)
	 */
	@Override
	public void process(Statement st) {
		
		numberOfTriples++;

		// collects owl:class, subclass and rdftype.
		if (st.getSubject().stringValue().startsWith("http")) {
			if (st.getSubject().stringValue().contains("dbpedia.org")) {
				numberOfDbpediaSubject++;
			}

		}
		
		if (st.getPredicate().stringValue().startsWith("http")) {
			if (st.getPredicate().stringValue().contains("dbpedia.org")) {
				numberOfDbpediaProperties++;
			}

		}
		
		if (st.getObject().stringValue().startsWith("http")) {
			if (st.getObject().stringValue().contains("dbpedia.org")) {
				numberOfDbpediaObjects++;
			}
		}
	}
	
}
