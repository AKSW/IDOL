/**
 * 
 */
package lodVader.mongodb.collections.adapters;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.ckanparser.CkanDatasetDB;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 28, 2016
 */
public class DatasetDBAdapter extends DatasetDB{

	/**
	 * Constructor for Class DatasetDBAdapter 
	 */
	public DatasetDBAdapter(CkanDatasetDB ckanDatasetDB, String datasource) {
		setUri(ckanDatasetDB.getCkanID());
		setIsVocabulary(false);
		setLabel(ckanDatasetDB.getTitle());
		setTitle(ckanDatasetDB.getTitle());
		addProvenance(datasource);
	}
	
}
