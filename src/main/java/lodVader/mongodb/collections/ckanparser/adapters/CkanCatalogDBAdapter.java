/**
 * 
 */
package lodVader.mongodb.collections.ckanparser.adapters;

import lodVader.mongodb.collections.ckanparser.CkanCatalogDB;
import lodVader.parsers.ckanparser.models.CkanCatalog;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 27, 2016
 */
public class CkanCatalogDBAdapter extends CkanCatalogDB{
	
	/**
	 * Constructor for Class CatalogAdapter 
	 */
	public CkanCatalogDBAdapter(CkanCatalog catalog) {
		setCatalogUrl(catalog.getCatalogAddress());
		setVersion(catalog.getVersion().toString());
	}

}
