package lodVader.mongodb.collections.RDFResources;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;

public class GeneralRDFResourceDB extends DBSuperClass {

	public static final String URI = "uri";
	
	public static enum COLLECTIONS {RESOURCES_ALL_PREDICATES, RESOURCES_RDF_TYPE, RESOURCES_OWL_CLASS, RESOURCES_RDF_SUBCLASS};
	
	public COLLECTIONS collection;

	public GeneralRDFResourceDB(COLLECTIONS collection) {
		super(collection.toString());
		this.collection = collection;
		setVariables();
	}

	public GeneralRDFResourceDB(COLLECTIONS collection, String uri) {
		super(collection.toString());
		setVariables();
	}

	public void setVariables() {
		addMandatoryField(URI);
	}

	public String getUri() {
		return getField(URI).toString();
	}

	public void setUri(String uri) {
		addField(URI, uri);
	}

	public List<GeneralRDFResourceDB> insertSet(Set<String> set) {
		List<GeneralRDFResourceDB> resources = new ArrayList<>();
		
		Iterator<String> i = set.iterator();
		List<DBObject> objects = new ArrayList<>();
		
		while (i.hasNext()) {
			GeneralRDFResourceDB resource = new GeneralRDFResourceDB(collection);
			resource.setUri(i.next().toString());
			resource.setID(new ObjectId().toString()); 
			objects.add(resource.mongoDBObject);
			resources.add(resource);
		} 
		
		bulkSave2(objects); 
		
		return resources;
	}

}
