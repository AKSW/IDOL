/**
 * 
 */
package lodVader.services.subsetDetection;

import java.util.HashMap;
import java.util.List;

import lodVader.mongodb.collections.DistributionDB;

/**
 * Interface for the implementation of subset detection. used to compare a source set with multiple target sets
 * @author Ciro Baron Neto
 * 
 * Oct 11, 2016
 */
public interface SubsetDetectorI {
	
	/**
	 * Set the main distribution
	 * @param sourceDistribution
	 */
	public void setSourceDistribution(DistributionDB sourceDistribution);
	
	/**
	 * Set the distributions which will be compared with the main distribution
	 * @param targetDistributions
	 */
	public void setTargetDistributions(List<String> targetDistributionsIds);
	
	/**
	 * Run the main method
	 */
	public void detectSubsets();
	
	/**
	 * Get the counters.
	 * @return counters The key is the distribution ID and the value is the intersection counter
	 */
	public HashMap<String, Double> getCounters();	
	
}
