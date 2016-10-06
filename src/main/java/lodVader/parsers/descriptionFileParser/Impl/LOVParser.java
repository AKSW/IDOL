/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.trentorise.opendata.jackan.ckan.CkanClient;
import eu.trentorise.opendata.jackan.ckan.CkanDataset;
import eu.trentorise.opendata.jackan.ckan.CkanResource;
import lodVader.enumerators.DistributionStatus;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.interfaces.DescriptionFileParserInterface;
import lodVader.utils.FormatsUtils;

/**
 * LOV parser. Using LOV api v2 (http://lov.okfn.org/dataset/lov/api/v2/)
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class LOVParser implements DescriptionFileParserInterface {

	final static Logger logger = LoggerFactory.getLogger(LOVParser.class);

	HashMap<String, DistributionDB> distributions = new HashMap<String, DistributionDB>();

	HashMap<String, DatasetDB> datasets = new HashMap<String, DatasetDB>();

	String repositoryAddress = "http://lov.okfn.org/dataset/lov/api/v2/vocabulary/list";
		

	/**
	 * Save a LOV Vocabulary or ontology instance the main collection
	 * 
	 * @param the
	 *            CkanDataset
	 * @return the DatasetDB instance
	 */
	public DatasetDB saveDataset(JSONObject object) {
		
		String title = ((JSONObject)((JSONArray)object.get("titles")).get(0)).get("value").toString();
		String url = object.get("uri").toString();		

		DatasetDB datasetDB = new DatasetDB(url);
		datasetDB.setIsVocabulary(true);
		datasetDB.setTitle(title);
		datasetDB.setLabel(title);
		datasetDB.setDescriptionFileParser(getParserName());
		datasetDB.addProvenance(repositoryAddress);
		logger.info("LOV Ontology/Vocabulary found: " + title);
		try {
			datasetDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		datasets.put(datasetDB.getUri(), datasetDB);
		DistributionDB distribution = saveDistribution(url, title, datasetDB);

		datasetDB.addDistributionID(distribution.getID());
		try {
			datasetDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return datasetDB;
	}

	/**
	 * Save a CkanResource instance the main collection (a distribution)
	 * 
	 * @param the
	 *            CkanResource
	 * @return the DistributionDB instance
	 */
	public DistributionDB saveDistribution(String url, String title, DatasetDB datasetDB) {

		DistributionDB distributionDB = new DistributionDB(url);
		distributionDB.setTitle(title);
		distributionDB.setUri(url);
		distributionDB.setIsVocabulary(true);
		distributionDB.setTopDataset(datasetDB.getID());
		distributionDB.setTopDatasetTitle(datasetDB.getTitle());
		distributionDB.setStatus(DistributionStatus.WAITING_TO_STREAM);
		try {
			distributionDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		distributions.put(distributionDB.getUri(), distributionDB);

		return distributionDB;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#
	 * getDistributions()
	 */
	@Override
	public List<DistributionDB> getDistributions() {
		return new ArrayList<DistributionDB>(distributions.values());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getDatasets()
	 */
	@Override
	public List<DatasetDB> getDatasets() {
		return new ArrayList<DatasetDB>(datasets.values());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {
		URI uri;
		try {
			uri = new URI(repositoryAddress);
			JSONTokener tokener = new JSONTokener(uri.toURL().openStream());
			JSONArray root = new JSONArray(tokener);
			
			Iterator<Object> it = root.iterator();
			while(it.hasNext()){
				saveDataset((JSONObject) it.next());
			}
			
		} catch (URISyntaxException | IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getParserName(
	 * )
	 */
	@Override
	public String getParserName() {
		return "LOV_PARSER";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#
	 * getRepositoryAddress()
	 */
	@Override
	public String getRepositoryAddress() {
		return repositoryAddress;
	}

}
