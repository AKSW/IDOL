/**
 * 
 */
package lodVader.mongodb.collections.ckanparser.adapters;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.ckanparser.CkanCatalogDB;
import lodVader.mongodb.collections.ckanparser.CkanDatasetDB;
import lodVader.mongodb.collections.ckanparser.CkanResourceDB;
import lodVader.parsers.ckanparser.models.CkanDataset;
import lodVader.parsers.ckanparser.models.CkanResource;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 27, 2016
 */
public class CkanDatasetDBAdapter extends CkanDatasetDB {

	/**
	 * Constructor for Class CatalogAdapter
	 */
	public CkanDatasetDBAdapter(CkanDataset dataset, String ckanCatalogID, String datasource) {
		setCatalog(ckanCatalogID);
		setCkanID(dataset.getId());
		setTitle(dataset.getTitle());
		try {
			update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (CkanResource resource : dataset.getResources()) {
			CkanResourceDB resourceDB = new CkanResourceDB();
			resourceDB.setCatalog(ckanCatalogID);
			resourceDB.setCkanDataset(getCkanID());
			resourceDB.addDataSource(datasource);
			resourceDB.setCkanID(resource.getId());
			resourceDB.setDownloadURL(resource.getUrl());
			resourceDB.setFormat(resource.getFormat());
			resourceDB.setTitle(resource.getTitle());
			try {
				resourceDB.update();
			} catch (LODVaderMissingPropertiesException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
