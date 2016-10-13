/**
 * 
 */
package lodVader.services.intersection;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.plugins.LODVaderPlugin;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 13, 2016
 */
public abstract class LODVaderIntersectionPlugin extends LODVaderPlugin implements LODVaderIntersectionAlgorithmI {
	
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
		getDB().addMandatoryField(IMPLEMENTATION);
		getDB().addMandatoryField(SOURCE_DISTRIBUTION);
		getDB().addMandatoryField(TARGET_DISTRIBUTION);
	}
	
	public void setValue(int value){
		getDB().addField(VALUE, value); 
	}

	public int getValue(){
		return ((Number) getDB().getField(VALUE)).intValue();
	}
	
	public void setSourceDistribution(DistributionDB sourceDistribution){
		if(sourceDistribution.getID() == null){
			sourceDistribution.find(true, DistributionDB.DOWNLOAD_URL, sourceDistribution.getDownloadUrl());
			getDB().addField(IMPLEMENTATION, implementationName);
		}
		getDB().addField(SOURCE_DISTRIBUTION, sourceDistribution.getID());
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
	
	

}
