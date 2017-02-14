/**
 * 
 */
package org.aksw.idol.core.mongodb;

import java.util.Arrays;
import java.util.List;

/**
 * @author Ciro Baron Neto
 * 
 * Nov 10, 2016
 */
public class DistributionModel {

	int opa = 123123123;
	
	String dae = "adsdadasd";
	
	List<String> list =  Arrays.asList("asdasdasd", "adasdasdasd");
	
	/**
	 * @return the dae
	 */
	public String getDae() {
		return dae;
	}
	
	/**
	 * @return the opa
	 */
	public int getOpa() {
		return opa;
	}
	
	/**
	 * @param dae 
	 * Set the dae value.
	 */
	public void setDae(String dae) {
		this.dae = dae;
	}
	/**
	 * @param opa 
	 * Set the opa value.
	 */
	public void setOpa(int opa) {
		this.opa = opa;
	}
	
}
