/**
 * 
 */
package org.aksw.idol.core.parsers.ckanparser.models;

/**
 * Class to hold CKAN's catalog information.
 * @author Ciro Baron Neto
 * 
 * Oct 27, 2016
 */
public class CkanCatalog {

	String catalogAddress;
	
	public static enum VERSION {V1, V2, V3};
	
	VERSION version;
	
	/**
	 * @param catalogAddress 
	 * Set the catalogAddress value.
	 */
	public void setCatalogAddress(String catalogAddress) {
		this.catalogAddress = catalogAddress;
	}
	
	/**
	 * @return the catalogAddress
	 */
	public String getCatalogAddress() {
		return catalogAddress;
	}
	
	/**
	 * @param version 
	 * Set the version value.
	 */
	public void setVersion(VERSION version) {
		this.version = version;
	}
	
	/**
	 * @return the version
	 */
	public VERSION getVersion() {
		return version;
	}
	
}
