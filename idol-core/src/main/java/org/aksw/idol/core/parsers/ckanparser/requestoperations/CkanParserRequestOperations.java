/**
 * 
 */
package org.aksw.idol.core.parsers.ckanparser.requestoperations;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 26, 2016
 */
public interface CkanParserRequestOperations {
	
	/**
	 * Make the dataset list request
	 * @param ckanCatalog
	 * @return
	 */
	public String makeDatasetListRequest(String ckanCatalog);

	/**
	 * Make the dataset request
	 * @param ckanCatalog
	 * @param datasetID
	 * @return
	 */
	public String makeDatasetRequest(String ckanCatalog, String datasetID);
	
	/**
	 * Make a dataset list request with pagination
	 * @param ckanCatalog
	 * @param rows
	 * @param start
	 * @return
	 */
	public String makeDatasetListRequestPagination(String ckanCatalog, int rows, int start);

}
