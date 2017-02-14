/**
 * 
 */
package org.aksw.idol.core.parsers.descriptionFileParser.helpers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
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
public class DataIDHelper {

	final static Logger logger = LoggerFactory.getLogger(DataIDHelper.class);

	private Model model = ModelFactory.createDefaultModel();

	/** 
	 * Load a DCAT model from a URL
	 * 
	 * @param URL
	 * @param format
	 * @return
	 */
	public boolean loadDataIDFile(String URL, String format) {

		FormatsUtils formatsUtils = new FormatsUtils();

		format = formatsUtils.getJenaFormat(format);

		logger.info("Loading DataID model from: " + URL.toString());

		HttpURLConnection URLConnection;
		try {
			URLConnection = (HttpURLConnection) new URL(URL).openConnection();
			URLConnection.setRequestProperty("Accept", "application/rdf+xml");
			model = null;
			model =  ModelFactory.createDefaultModel();
			model.read(URLConnection.getInputStream(), null, format);

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * Get the primary topic URI
	 * 
	 * @return primary topic URI
	 */
	public String getPrimaryTopic() {

		// find primaryTopic
		StmtIterator datasetsStmt = model.listStatements(null, RDFResourcesTags.primaryTopic, (RDFNode) null);

		if (datasetsStmt.hasNext())
			return datasetsStmt.next().getObject().toString();
		return null;
	}

	/**
	 * Get format from a distribution
	 * 
	 * @param CKAN_ID
	 * @return
	 */
	public String getFormat(String distribution) {
		StmtIterator stmtFormat = model.listStatements(model.createResource(distribution), RDFResourcesTags.format,
				(RDFNode) null);
		if (stmtFormat.hasNext()) {
			return stmtFormat.next().getObject().toString();
		}
		return "";
	}

	/**
	 * Get a list of subsets of a dataset
	 * 
	 * @return the array of subsets URIs
	 */
	public List<String> getSubsets(String dataset) {

		List<String> subsets = new ArrayList<String>();

		StmtIterator stmtSubset = model.listStatements(model.createResource(dataset), RDFResourcesTags.subset,
				(RDFNode) null);

		while (stmtSubset.hasNext()) {
			subsets.add(stmtSubset.next().getObject().toString());
		}
		return subsets;
	}

	/**
	 * Get a list of distributions of a dataset
	 * 
	 * @return the array of subsets URIs
	 */
	public List<String> getDistributions(String dataset) {

		List<String> distributions = new ArrayList<String>();

		HashSet<String> distributionsTmp = new HashSet<>();

//		StmtIterator stmtDistributions = model.listStatements(model.createResource(dataset),
//				RDFResourcesTags.dcatDistribution, (RDFNode) null);
		StmtIterator stmtDistributions = model.listStatements(null ,
				RDFResourcesTags.dcatDownloadURL, (RDFNode) null);

		while (stmtDistributions.hasNext()) {

			RDFNode object = stmtDistributions.next().getObject();

			// check if it's not a sparql endpoint
			// if (model.listStatements(object.asResource(),
			// RDFResourcesTags.type,
			// RDFResourcesTags.dataIDSingleFile).hasNext()) {
//			if (model.listStatements(null, RDFResourcesTags.dcatDownloadURL, (RDFNode) null)
//					.hasNext()) {

				if (!distributionsTmp.contains(object.toString().replace(".bz2", "").replace(".ttl", "")
						.replace(".tql", "").replace(".nt", ""))) {
					distributions.add(object.toString());
					distributionsTmp.add(object.toString().replace(".bz2", "").replace(".ttl", "").replace(".tql", "")
							.replace(".nt", ""));
//				}
			}

		}
		return distributions;
	}

	/**
	 * Get title from a dataset, subset or distribution
	 * 
	 * @param URI
	 * @return
	 */
	public String getTitle(String URI) {

		StmtIterator stmtTitle = model.listStatements(model.createResource(URI), RDFResourcesTags.title,
				(RDFNode) null);

		if (stmtTitle.hasNext())
			return stmtTitle.next().getObject().toString();
		return "";

	}

	/**
	 * Get label from a dataset, subset or distribution
	 * 
	 * @param URI
	 * @return the label
	 */
	public String getLabel(String URI) {

		StmtIterator stmtLabel = model.listStatements(model.createResource(URI), RDFResourcesTags.label,
				(RDFNode) null);

		if (stmtLabel.hasNext())
			return stmtLabel.next().getObject().toString();
		return "";

	}

	/**
	 * Get downloadURL from a distribution
	 * 
	 * @param distribution
	 * @return the downloadURL String
	 */
	public String getDownloadURL(String distribution) {

		StmtIterator stmtdownloadURL = model.listStatements(model.createResource(distribution),
				RDFResourcesTags.dcatDownloadURL, (RDFNode) null);

		if (stmtdownloadURL.hasNext())
			return stmtdownloadURL.next().getObject().toString();
		return "";

	}

}
