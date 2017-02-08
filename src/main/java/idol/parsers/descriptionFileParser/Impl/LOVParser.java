/**
 * 
 */
package idol.parsers.descriptionFileParser.Impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import idol.exceptions.LODVaderMissingPropertiesException;
import idol.mongodb.collections.DatasetDB;
import idol.mongodb.collections.DistributionDB;
import idol.mongodb.collections.DistributionDB.DistributionStatus;
import idol.parsers.descriptionFileParser.MetadataParser;
import idol.parsers.descriptionFileParser.MetadataParserI;

/**
 * LOV parser. Using LOV api v2 (http://lov.okfn.org/dataset/lov/api/v2/)
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class LOVParser extends MetadataParser{

	/**
	 * Constructor for Class LOVParser 
	 * @param parserName
	 */
	public LOVParser() {
		super("LOV_PARSER");
	}

	final static Logger logger = LoggerFactory.getLogger(LOVParser.class);


	String repositoryAddress = "http://lov.okfn.org/dataset/lov/api/v2/vocabulary/list";

	/**
	 * Save a LOV Vocabulary or ontology instance the main collection
	 * 
	 * @param the
	 *            CkanDataset
	 * @return the DatasetDB instance
	 */
	public DatasetDB saveDataset(JSONObject object) {

		String title = ((JSONObject) ((JSONArray) object.get("titles")).get(0)).get("value").toString();
		String url = object.get("uri").toString();
		
		DatasetDB dataset = addDataset(url, true, title, title, getParserName());

		saveDistribution(url, title, dataset);
		
		return dataset;
	}

	/**
	 * Save a CkanResource instance the main collection (a distribution)
	 * 
	 * @param the
	 *            CkanResource
	 * @return the DistributionDB instance
	 */
	public DistributionDB saveDistribution(String url, String title, DatasetDB datasetDB) {

		String downloadURL = "http://lov.okfn.org/dataset/lov/sparql?query="
				+ URLEncoder.encode("CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + url + "> { ?s ?p ?o } }");
		
		return addDistribution(downloadURL, true, title, "ttl", downloadURL,
				datasetDB.getID(), datasetDB.getTitle(), getParserName(), repositoryAddress, null, null);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see idol.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {
		URI uri;
		try {
			uri = new URI(repositoryAddress);
			JSONTokener tokener = new JSONTokener(uri.toURL().openStream());
			JSONArray root = new JSONArray(tokener);

			Iterator<Object> it = root.iterator();
			while (it.hasNext()) {
				saveDataset((JSONObject) it.next());
			}

		} catch (URISyntaxException | IOException e) {
			e.printStackTrace();
		}
	}

}
