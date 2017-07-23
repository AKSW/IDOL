/**
 * 
 */
package org.aksw.idol.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Ciro Baron Neto
 * 
 *         Jul 22, 2017
 */

@ConfigurationProperties
public class Properties {

	public IDOLProperties idolproperties = new IDOLProperties();

	public IDOLProperties getIdolproperties() {
		return idolproperties;
	}

	public void setIdolproperties(IDOLProperties idolproperties) {
		this.idolproperties = idolproperties;
	}
}
