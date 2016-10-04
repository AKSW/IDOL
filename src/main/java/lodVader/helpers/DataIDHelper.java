/**
 * 
 */
package lodVader.helpers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import lodVader.ontology.RDFProperties;
import lodVader.utils.FormatsUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class DataIDHelper {

	final static Logger logger = LoggerFactory.getLogger(DataIDHelper.class);

	private Model dataIDModel = ModelFactory.createDefaultModel();

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
			dataIDModel.read(URLConnection.getInputStream(), null, format);

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
		StmtIterator datasetsStmt = dataIDModel.listStatements(null, RDFProperties.primaryTopic, (RDFNode) null);

		if (datasetsStmt.hasNext())
			return datasetsStmt.next().getObject().toString();
		return null;

	}

	/**
	 * Get a list of subsets of a dataset
	 * 
	 * @return the array of subsets URIs
	 */
	public List<String> getSubsets(String dataset) {

		List<String> subsets = new ArrayList<String>();

		StmtIterator stmtSubset = dataIDModel.listStatements(dataIDModel.createResource(dataset), RDFProperties.subset,
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

		StmtIterator stmtDistributions = dataIDModel.listStatements(dataIDModel.createResource(dataset),
				RDFProperties.dcatDistribution, (RDFNode) null);

		while (stmtDistributions.hasNext()) {
			
			RDFNode object = stmtDistributions.next().getObject(); 

			// check if it's not a sparql endpoint
			if (dataIDModel.listStatements(object.asResource(), RDFProperties.type,
					RDFProperties.dataIDSingleFile).hasNext()) {
				distributions.add(object.toString());
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

		StmtIterator stmtTitle = dataIDModel.listStatements(dataIDModel.createResource(URI), RDFProperties.title,
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

		StmtIterator stmtLabel = dataIDModel.listStatements(dataIDModel.createResource(URI), RDFProperties.label,
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

		System.out.println(distribution);
		StmtIterator stmtdownloadURL = dataIDModel.listStatements(dataIDModel.createResource(distribution),
				RDFProperties.dcatDownloadURL, (RDFNode) null);

		if (stmtdownloadURL.hasNext())
			return stmtdownloadURL.next().getObject().toString();
		return "";

	}

}
