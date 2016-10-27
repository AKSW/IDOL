package lodVader.mongodb.collections.ckanparser;

import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;

public class CkanCatalogDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "CkanCatalog";

	public static final String CATALOG_URL = "catalogUrl";
	
	public static final String VERSION = "version";


	public String provinance;


	public CkanCatalogDB(DBObject object) {
		super(COLLECTION_NAME);
		setKeys();
		mongoDBObject = object;
	}
	
	/**
	 * Constructor for Class DatasetDB 
	 */
	public CkanCatalogDB() {
		super(COLLECTION_NAME);
		setKeys();
	}
	
	/**
	 * Constructor for Class DatasetDB 
	 */
	public CkanCatalogDB(String id) {
		super(COLLECTION_NAME);
		setKeys();
		find(true, VERSION, id);
	}
	

	public void setKeys() {
		addMandatoryField(VERSION);
		addMandatoryField(CATALOG_URL);
	}
	
	public void setCatalogUrl(String catalogUrl){
		addField(CATALOG_URL, catalogUrl);
	}
	
	/**
	 * @return the provenance
	 */
	public String getCatalogUrl() {
		return getField(CATALOG_URL).toString();
	}

	
	public String getVersion() {
		return getField(VERSION).toString();
	}

	public void setVersion(String version) {
		addField(VERSION, version);
	}
	

}
