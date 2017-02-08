package idol.mongodb.collections.ckanparser;

import java.util.ArrayList;

import com.mongodb.DBObject;

import idol.mongodb.DBSuperClass;

public class CkanResourceDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "CkanResource";

	public static final String CKAN_CATALOG = "ckanCatalog";

	public static final String CKAN_DATASET = "ckanDataset";

	public static final String CKAN_ID = "ckanId";

	public static final String TITLE = "title";

	public static final String FORMAT = "format";

	public static final String DOWNLOAD_URL = "downloadURL";

	public static final String DATASOURCE = "datasource";

	public String provinance;

	public CkanResourceDB(DBObject object) {
		super(COLLECTION_NAME);
		setKeys();
		mongoDBObject = object;
	}

	/**
	 * Constructor for Class DatasetDB
	 */
	public CkanResourceDB() {
		super(COLLECTION_NAME);
		setKeys();
	}

	/**
	 * Constructor for Class DatasetDB
	 */
	public CkanResourceDB(String id) {
		super(COLLECTION_NAME);
		setKeys();
		setCkanID(id);
		find(true, CKAN_ID, id);
	}

	public void setKeys() {
		// addMandatoryField(DOWNLOAD_URL);
		addMandatoryField(CKAN_CATALOG);
		addMandatoryField(CKAN_DATASET);
		// addMandatoryField(FORMAT);
		// addMandatoryField(TITLE);
		addMandatoryField(CKAN_ID);
	}

	/**
	 * @return the provenance
	 */
	public String getCatalog() {
		return getField(CKAN_CATALOG).toString();
	}

	public String getTitle() {
		try {
			return getField(TITLE).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void addDataSource(String datasource) {
		ArrayList<String> ids = (ArrayList<String>) getField(DATASOURCE);
		if (ids != null) {
			if (!ids.contains(datasource)) {
				ids.add(datasource);
				addField(DATASOURCE, ids);
			}
		} else {
			ids = new ArrayList<String>();
			ids.add(datasource);
			addField(DATASOURCE, ids);
		}
	}
	
	public ArrayList<String> getDataSources() {
		return (ArrayList<String>) getField(DATASOURCE);
	}
	
	
	public void setTitle(String title) {
		addField(TITLE, title);
	}

	public void setCkanDataset(String datasetId) {
		addField(CKAN_DATASET, datasetId);
	}

	public String getCkanDataset() {
		return getField(CKAN_DATASET).toString();
	}

	public void setDownloadURL(String downloadURL) {
		addField(DOWNLOAD_URL, downloadURL);
	}

	public String getDownloadURL() {
		return getField(DOWNLOAD_URL).toString();
	}

	public void setCatalog(String catalog) {
		addField(CKAN_CATALOG, catalog);
	}

	public void setFormat(String format) {
		addField(FORMAT, format);
	}

	public String getFormat() {
		if (getField(FORMAT) != null)
			return getField(FORMAT).toString();
		else
			return null;
	}

	public String getCkanID() {
		return getField(CKAN_ID).toString();
	}

	public void setCkanID(String id) {
		addField(CKAN_ID, id);
	}

}
