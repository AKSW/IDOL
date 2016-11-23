/**
 * 
 */
package lodVader.mongodb.collections;

import java.net.MalformedURLException;
import java.util.ArrayList;

import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.utils.URLUtils;

/**
 * @author Ciro Baron Neto
 * 
 * Nov 21, 2016
 */
public class DistributionDumpStats  extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "DistributionDumpStats";

	public DistributionDumpStats() {
		super(COLLECTION_NAME);
		setKeys();
	}

	public DistributionDumpStats(String distributionID) {
		super(COLLECTION_NAME);
		
		setKeys();
		find(true, DISTRIBUTION_ID, distributionID);
	}

	public DistributionDumpStats(DBObject object) {
		super(COLLECTION_NAME);
		mongoDBObject = object;
		setKeys();
	}

	private void setKeys() {
		addMandatoryField(DISTRIBUTION_ID);
	}

	// collection properties
	public static final String DISTRIBUTION_ID = "distributionID";

	public static final String UNIQ_TRIPLES = "uniqTriples";

	public static final String TOTAL_TRIPLES = "totalTriples";

	public static final String DATASOURCE_UNIQ_LAST_MSG = "datasourceUniqLastMsg";

	public static final String GLOBAL_UNIQ_LAST_MSG = "globalUniqLastMsg";

	public static final String FILE_LAST_MSG = "fileLastMsg";
	
	public static final String DATASOURCE_UNIQ_STATUS = "datasourceUniqStatus";
	
	public static final String GLOBAL_UNIQ_STATUS = "globalUniqStatus";
	
	public static final String FILE_STATUS = "fileStatus";	


	public enum DatasourceUniqStatus {
		ERROR,
		DONE,
	}
	
	public enum GlobalUniqStatus {
		ERROR,
		DONE,
	}
	
	public enum FileStatus {
		ERROR,
		DONE,
	}

	

}
