/**
 * 
 */
package org.aksw.idol.mongodb.collections.ckanparser.adapters;

import org.aksw.idol.mongodb.collections.ckanparser.CkanCatalogDB;
import org.aksw.idol.parsers.ckanparser.models.CkanCatalog;

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
