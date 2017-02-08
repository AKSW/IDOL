/**
 * 
 */
package idol.plugins.intersection;

import java.util.HashMap;
import java.util.List;

import org.hibernate.validator.internal.util.privilegedactions.GetMethodFromPropertyName;

import idol.mongodb.collections.DistributionDB;
import idol.plugins.LODVaderPlugin;

/**
 * Interface for the implementation of subset detection. used to compare a
 * source set with multiple target sets
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public interface LODVaderIntersectionAlgorithmI {

	/**
	 * Run the main method
	 * 
	 * @param sourceDistribution
	 *            the main distribution
	 * @param distributions
	 *            which will be compared with the main distribution
	 * @return counters The key is the distribution ID and the value is the
	 *         intersection counter
	 */
	public HashMap<String, Double> runDetection(DistributionDB sourceDistribution,
			List<String> targetDistributionsIDs);

}
