/**
 * 
 */
package org.aksw.idol.core.parsers.descriptionFileParser.helpers;

import java.util.ArrayList;
import java.util.List;

import org.aksw.idol.core.ontology.RDFResourcesTags;
import org.aksw.idol.utils.FormatsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class LodCloudHelper {

	final static Logger logger = LoggerFactory.getLogger(LodCloudHelper.class);

	private Model model = ModelFactory.createDefaultModel();

	/**
	 * Constructor for Class LodCloudHelper 
	 */
	public LodCloudHelper(Model model) {
		this.model = model;
	}

	/**
	 * Get a list of datasets
	 * 
	 * @return the array of datasets URIs
	 */
	public List<String> getDatasets() {

		List<String> datasets = new ArrayList<String>();

		StmtIterator stmt = model.listStatements(null, RDFResourcesTags.type, RDFResourcesTags.dcatDatasetResource);

		while (stmt.hasNext()) {
			datasets.add(stmt.next().getSubject().toString());
		}
		return datasets;
	}

	/**
	 * Get a list of distributions of a dataset
	 * 
	 * @return the array of subsets URIs
	 */
	public List<RDFNode> getDistributions(String dataset) {

		List<RDFNode> distributions = new ArrayList<RDFNode>();

		StmtIterator stmtDistributions = model.listStatements(model.createResource(dataset),
				RDFResourcesTags.dcatDistribution, (RDFNode) null);

		while (stmtDistributions.hasNext()) {
			distributions.add(stmtDistributions.next().getObject());
		}
		return distributions;
	}

	/**
	 * Get format of a distribution
	 * 
	 * @param URI
	 * @return
	 */
	public String getFormat(RDFNode distribution) {
		StmtIterator stmtFormat = model.listStatements((distribution).asResource(), RDFResourcesTags.format,
				(RDFNode) null);
		if (stmtFormat.hasNext()) {
			return model
					.listStatements(stmtFormat.next().getObject().asResource(), RDFResourcesTags.rdfValue, (RDFNode) null)
					.next().getObject().toString();
		}
		return "";
	}
	
	/**
	 * Get format of a distribution
	 * 
	 * @param URI
	 * @return
	 */
	public String getFormat2(RDFNode distribution) {
		StmtIterator stmtFormat = model.listStatements((distribution).asResource(), RDFResourcesTags.format_1,
				(RDFNode) null);
		if (stmtFormat.hasNext()) {
//			System.out.println(model
//					.listStatements(stmtFormat.next().getObject().asResource(), RDFResourcesTags.rdfValue, (RDFNode) null)
//					.next().getObject().toString());
//			return "";
			return FormatsUtils.getEquivalentFormat(model
					.listStatements(stmtFormat.next().getObject().asResource(), RDFResourcesTags.rdfValue, (RDFNode) null)
					.next().getObject().toString());
		}
		return "";
	}
	
	/**
	 * Get accessURL from a distribution
	 * 
	 * @param URI
	 * @return
	 */
	public String getAccessURL(RDFNode distribution) {
		StmtIterator stmtFormat = model.listStatements((distribution).asResource(), RDFResourcesTags.dcatAccessURL,
				(RDFNode) null);
		if (stmtFormat.hasNext()) {
			return stmtFormat.next().getObject().toString();
		}
		return "";
	}

	/**
	 * Get title from a dataset, subset or distribution
	 * 
	 * @param URI
	 * @return
	 */
	public String getTitle(String dataset) {

		StmtIterator stmtTitle = model.listStatements(model.createResource(dataset), RDFResourcesTags.title, (RDFNode) null);

		if (stmtTitle.hasNext())
			return stmtTitle.next().getObject().toString();
		return "";

	}
}
