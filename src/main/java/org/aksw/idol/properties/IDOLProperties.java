/**
 * 
 */
package org.aksw.idol.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author Ciro Baron Neto
 * 
 * Jul 22, 2017
 */

@ConfigurationProperties
public class IDOLProperties {
	
	String p;
	
	public String getP() {
		return p;
	}
	
	public void setP(String p) {
		this.p = p;
	}
	
}
