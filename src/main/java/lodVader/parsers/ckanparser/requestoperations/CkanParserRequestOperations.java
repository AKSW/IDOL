/**
 * 
 */
package lodVader.parsers.ckanparser.requestoperations;

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

}
