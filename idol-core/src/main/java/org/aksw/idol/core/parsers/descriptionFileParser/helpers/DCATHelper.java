/**
 * 
 */
package org.aksw.idol.core.parsers.descriptionFileParser.helpers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
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
public class DCATHelper {

	final static Logger logger = LoggerFactory.getLogger(DCATHelper.class);

	private Model dcatModel = ModelFactory.createDefaultModel();
	
	/**
	 * Constructor for Class DCATHelper 
	 */
	public DCATHelper(String URI, String format){
		loadDCATFile(URI, format); 
	}
	
	/**
	 * Constructor for Class DCATHelper 
	 */
	private DCATHelper() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Load a DCAT model from a URL
	 * 
	 * @param URL
	 * @param format
	 * @return
	 */
	public boolean loadDCATFile(String URL, String format) {

		FormatsUtils formatsUtils = new FormatsUtils();

		format = formatsUtils.getJenaFormat(format);

		logger.info("Loading DCAT model from: " + URL.toString());

		HttpURLConnection URLConnection;
		try {
			URLConnection = (HttpURLConnection) new URL(URL).openConnection();
			URLConnection.setRequestProperty("Accept", "application/rdf+xml");
			dcatModel.read(URLConnection.getInputStream(), null, format);

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * Get a list of catalogs URIs available in the dcat file
	 * @return list of catalogs URIs
	 */
	public List<String> getListOfCatalogs() {
		List<String> catalogs = new ArrayList<String>();

		// get uri for the catalog
		StmtIterator stmtCatalogs = dcatModel.listStatements(null, RDFResourcesTags.type, RDFResourcesTags.dcatCatalog);

		while (stmtCatalogs.hasNext()) {
			catalogs.add(stmtCatalogs.next().getSubject().toString());
		}

		return catalogs;
	}

	/**
	 * Get a list an array of datasets URIs from a catalog
	 * 
	 * @return the array of datasets URIs
	 */
	public List<String> getDatasetsFromCatalog(String catalog) {

		List<String> datasets = new ArrayList<String>();

		StmtIterator stmtDatasets = dcatModel.listStatements(dcatModel.createResource(catalog),
				RDFResourcesTags.dcatRecord, (RDFNode) null);

		while (stmtDatasets.hasNext()) {
			datasets.add(stmtDatasets.next().getObject().toString());
		}

		return datasets;
	}

}
