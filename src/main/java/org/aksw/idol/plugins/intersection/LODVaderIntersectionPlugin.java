/**
 * 
 */
package org.aksw.idol.plugins.intersection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.plugins.LODVaderPlugin;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 13, 2016
 */
public abstract class LODVaderIntersectionPlugin extends LODVaderPlugin {
	
	public String implementationName;

	/**
	 * Constructor for Class LODVaderIntersectionPlugin 
	 * @param pluginName
	 */
	public LODVaderIntersectionPlugin(String implemantationName) {
		super("INTERSECTION_PLUGIN");
		this.implementationName = implemantationName;
	}
	
	
	/**
	 * MongoDB fields
	 */
	public static final String VALUE = "value";
	
	public static final String SOURCE_DISTRIBUTION = "sourceDistribution";
	
	public static final String TARGET_DISTRIBUTION = "targetDistribution";

	public static final String IMPLEMENTATION = "implementation";

	
	
	public void setVariables() {
		getDB().addMandatoryField(SOURCE_DISTRIBUTION);
		getDB().addMandatoryField(TARGET_DISTRIBUTION);
	}
	
	public void setValue(int value){
		getDB().addField(VALUE, value); 
	}

	public int getValue(){
		return ((Number) getDB().getField(VALUE)).intValue();
	}
	

	public void setSourceDistribution(String sourceDistribution){
		getDB().addField(SOURCE_DISTRIBUTION, sourceDistribution);
	}
	
	public void setTargetDistribution(String targetDistribution){
		getDB().addField(TARGET_DISTRIBUTION, targetDistribution);
	}
	
	
	public String getTargetDistribution() {
		return getDB().getField(TARGET_DISTRIBUTION).toString();
	}
	
	public String getSourceDistribution() {
		return getDB().getField(SOURCE_DISTRIBUTION).toString();
	}
	
	public void save(HashMap<String, Long> results, String sourceDistribution){

		List<DBObject> list = new ArrayList<>();

		for (String result : results.keySet()) {
			DBObject object = new BasicDBObject();
			object.put(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION, sourceDistribution);
			object.put(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION, result);
			object.put(LODVaderIntersectionPlugin.VALUE, results.get(result).intValue());
			object.put(LODVaderIntersectionPlugin.IMPLEMENTATION, implementationName);
			list.add(object);
		}
		
		DBObject object = new BasicDBObject();
		object.put(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION, sourceDistribution);
		object.put(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION, sourceDistribution);
		object.put(LODVaderIntersectionPlugin.VALUE, -1);
		object.put(LODVaderIntersectionPlugin.IMPLEMENTATION, implementationName);
		list.add(object);
		
		getDB().bulkSave2(list);
		
		
	}
	

}
