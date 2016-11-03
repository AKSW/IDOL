/**
 * 
 */
package lodVader.parsers.ckanparser.requestoperations.impl;

import lodVader.parsers.ckanparser.requestoperations.CkanParserRequestOperations;

/**
 * Implementation of CKAN V3 requests
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 26, 2016
 */
public class V3 implements CkanParserRequestOperations {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.ckanparser.CkanParserOperations#makeDatasetListAddress(
	 * java.lang.String)
	 */
	@Override
	public String makeDatasetListRequest(String ckanCatalog) {
		return ckanCatalog + "/api/3/action/package_list";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.ckanparser.CkanParserOperations#makeDatasetAddress(java.
	 * lang.String, java.lang.String)
	 */
	@Override
	public String makeDatasetRequest(String ckanCatalog, String datasetID) {
		return ckanCatalog + "/api/3/action/package_show?id=" + datasetID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.ckanparser.requestoperations.CkanParserRequestOperations
	 * #makeDatasetListRequestPagination(java.lang.String, int, int)
	 */
	@Override
	public String makeDatasetListRequestPagination(String ckanCatalog, int rows, int start) {
		return ckanCatalog + "/api/3/action/package_search?rows=" + rows + "&start=" + start;
	}

}
