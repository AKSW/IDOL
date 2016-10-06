/**
 * 
 */
package lodVader.services.mongodb.distribution;

import java.util.List;

import lodVader.mongodb.collections.DistributionDB;

/**
 * @author Ciro Baron Neto
 * 
 * Sep 11, 2016
 */
public class DistributionServices {
	
	public void saveAllDistributions(List<DistributionDB> distributions){
		distributions.forEach((distribution)->{
			try {
				distribution.update();
			} catch (Exception e) {
				e.printStackTrace();
			} 
		});
	}

}
