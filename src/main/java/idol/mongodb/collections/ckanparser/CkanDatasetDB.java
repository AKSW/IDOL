package idol.mongodb.collections.ckanparser;

import java.util.ArrayList;

import com.mongodb.DBObject;

import idol.mongodb.DBSuperClass;

public class CkanDatasetDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "CkanDataset";

	public static final String CKAN_CATALOG = "ckanCatalog";
	
	public static final String CKAN_ID = "ckanId";

	public static final String TITLE = "title";

	public String provinance;


	public CkanDatasetDB(DBObject object) {
		super(COLLECTION_NAME);
		setKeys();
		mongoDBObject = object;
	}
	
	/**
	 * Constructor for Class DatasetDB 
	 */
	public CkanDatasetDB() {
		super(COLLECTION_NAME);
		setKeys();
	}
	
	/**
	 * Constructor for Class DatasetDB 
	 */
	public CkanDatasetDB(String id) {
		super(COLLECTION_NAME);
		setKeys();
		setCkanID(id);
		find(true, CKAN_ID, id);
	}
	

	public void setKeys() {
		addMandatoryField(CKAN_ID);
		addMandatoryField(CKAN_CATALOG);
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

	public void setTitle(String title) {
		addField(TITLE, title);
	}
	
	public void setCatalog(String catalog){
		addField(CKAN_CATALOG, catalog);
	}
	


	public String getCkanID() {
		return getField(CKAN_ID).toString();
	}

	public void setCkanID(String id) {
		addField(CKAN_ID, id);
	}
	

}
