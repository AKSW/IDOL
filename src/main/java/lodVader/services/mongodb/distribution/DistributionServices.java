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
	
	public void saveAllDistributions(List<DistributionDB> distributions, String repository, String datasource){
		distributions.forEach((distribution)->{
			try {
				distribution.addDatasource(datasource);
				distribution.addRepository(repository);
				distribution.update();
			} catch (Exception e) {
				e.printStackTrace();
			} 
		});
	}

}
