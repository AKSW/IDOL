/**
 * 
 */
package org.aksw.idol.plugins;

import org.aksw.idol.mongodb.DBSuperClass;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 13, 2016
 */
public abstract class LODVaderPlugin {

	String pluginName;

	String dbName;

	/**
	 * Constructor for Class LODVaderPlugin
	 */
	public LODVaderPlugin(String pluginName) {
		this.pluginName = pluginName;
	}

	/**
	 * @return the pluginName
	 */
	public String getPluginName() {
		return pluginName;
	}

	/**
	 * @param pluginName
	 *            Set the pluginName value.
	 */
	public void setPluginName(String pluginName) {
		this.pluginName = pluginName;
	}


	private DBSuperClass db;

	public DBSuperClass getDB() {
		if (db == null) {
			db = new DBSuperClass("PLUGIN_" + getPluginName());
		}
		return db;
	}

}
