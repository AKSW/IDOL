/**
 * 
 */
package lodVader.parsers.ckanparser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.parsers.ckanparser.helpers.HTTPConnectionHelper;
import lodVader.parsers.ckanparser.models.CkanCatalog;
import lodVader.parsers.ckanparser.models.CkanDataset;
import lodVader.parsers.ckanparser.models.CkanDatasetAdapterV3;
import lodVader.parsers.ckanparser.requestoperations.CkanParserRequestOperations;
import lodVader.parsers.ckanparser.requestoperations.impl.V3;
import lodVader.parsers.ckanparser.utils.CkanUtils;

/**
 * CKAN parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 26, 2016
 */
public class CkanParser implements CkanParserInterface {

	final static Logger logger = LoggerFactory.getLogger(CkanParser.class);

	
	// For each CKAN version the request operations are different.
	CkanParserRequestOperations operations;

	// Class which holds all CKAN catalog information
	CkanCatalog ckanCatalog = new CkanCatalog();

	HTTPConnectionHelper connectionHelper = new HTTPConnectionHelper();

	List<String> datasetIds;

	CkanDatasetList datasetList = new CkanDatasetList(this);

	/**
	 * Constructor for Class CkanParser
	 */
	public CkanParser(String ckanCatalogAddress) {
		this.ckanCatalog.setCatalogAddress(ckanCatalogAddress);
	}
	
	/**
	 * @return the ckanCatalog
	 */
	public CkanCatalog getCkanCatalog() {
		getOperations();
		return ckanCatalog;
	}

	private List<String> getPackageListFromCatalog() {
		try {
			datasetIds = new ArrayList<>();
			connectionHelper.getJSONResponse(getOperations().makeDatasetListRequest(ckanCatalog.getCatalogAddress()))
					.getJSONArray("result").forEach((id) -> {
						datasetIds.add(id.toString());
					});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return datasetIds;
	}

	/**
	 * @return the operations
	 */
	public CkanParserRequestOperations getOperations() {
		if (operations == null) {
			try {
				if (operations == null) {
					if (CkanUtils.unveil(ckanCatalog.getCatalogAddress()).equals("3")) {
						operations = new V3();
						ckanCatalog.setVersion(CkanCatalog.VERSION.V3);
					}
				}
			} catch (JSONException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return operations;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.ckanparser.CkanParserInterface#fetchDataset(java.lang.
	 * String)
	 */
	@Override
	public CkanDataset fetchDataset(String datasetID) {
		CkanDataset ckanDataset = new CkanDataset();

		try {
			ckanDataset = new CkanDatasetAdapterV3(
					new JSONObject(connectionHelper
							.getJSONResponse(
									getOperations().makeDatasetRequest(ckanCatalog.getCatalogAddress(), datasetID))
							.get("result").toString()));
		} catch (JSONException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("Loaded dataset: "+ ckanDataset.getId());
		return ckanDataset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.ckanparser.CkanParserInterface#fetchDatasetIds()
	 */
	@Override
	public List<String> fetchDatasetIds() {
		if (datasetIds == null)
			getPackageListFromCatalog();
		return datasetIds;
	}

	/**
	 * @return the datasetList
	 */
	public CkanDatasetList getDatasetList() {
		getOperations();
		return datasetList;
	}

}
