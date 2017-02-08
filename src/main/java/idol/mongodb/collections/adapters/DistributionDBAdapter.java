/**
 * 
 */
package idol.mongodb.collections.adapters;

import java.net.MalformedURLException;

import com.hp.hpl.jena.sparql.core.DatasetGraphReadOnly;

import idol.mongodb.collections.DistributionDB;
import idol.mongodb.collections.ckanparser.CkanResourceDB;
import idol.utils.FormatsUtils;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 28, 2016
 */
public class DistributionDBAdapter extends DistributionDB{

	
	/**
	 * Constructor for Class DistributionDBAdapter 
	 */
	public DistributionDBAdapter(CkanResourceDB ckanResourceDB, String datasource) {
		try {
			setUri(ckanResourceDB.getCkanID());
			setFormat(FormatsUtils.getEquivalentFormat(ckanResourceDB.getFormat()));
			setDownloadUrl(ckanResourceDB.getDownloadURL());
			setTitle(ckanResourceDB.getTitle());
			setIsVocabulary(false);
			setLabel(ckanResourceDB.getTitle());
			setStatus(DistributionStatus.WAITING_TO_STREAM);
			addRepository(ckanResourceDB.getCatalog());
			addDatasource(datasource);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		
	}
	
}
