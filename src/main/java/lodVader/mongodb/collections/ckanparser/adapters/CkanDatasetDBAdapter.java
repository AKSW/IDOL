/**
 * 
 */
package lodVader.mongodb.collections.ckanparser.adapters;

import java.util.ArrayList;

import com.hp.hpl.jena.xmloutput.impl.Basic;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.ckanparser.CkanCatalogDB;
import lodVader.mongodb.collections.ckanparser.CkanDatasetDB;
import lodVader.mongodb.collections.ckanparser.CkanResourceDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
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
			ArrayList<DBObject> objects = new GeneralQueriesHelper().getObjects(CkanResourceDB.COLLECTION_NAME, new BasicDBObject(CkanResourceDB.CKAN_ID, resource.getId()));
			if(objects.size()>0){
				resourceDB = new CkanResourceDB(objects.iterator().next());
			}
			
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
