/**
 * 
 */
package lodVader.parsers.ckanparser;

import java.util.List;

import lodVader.parsers.ckanparser.models.CkanDataset;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 27, 2016
 */
public interface CkanParserInterface {
	
	/**
	 * Given a CKAN id, fetch data from te catalog and returns a CKAN dataset instance
	 * @param datasetID
	 * @return the Ckan dataset
	 */
	public CkanDataset fetchDataset(String datasetID);
	
	
	/**
	 * Fetch dataset ids from the catalog
	 * @return the datasetIds
	 */
	public List<String> fetchDatasetIds();
	

}
