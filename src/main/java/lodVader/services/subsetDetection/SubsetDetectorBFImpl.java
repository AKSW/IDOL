/**
 * 
 */
package lodVader.services.subsetDetection;

import java.util.HashMap;
import java.util.List;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.mongodb.collections.datasetBF.BucketDBHelper;
import lodVader.services.mongodb.resourceRelation.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 11, 2016
 */
public class SubsetDetectorBFImpl implements SubsetDetectorI {

	DistributionDB sourceDistribution;
	
	List<String> targetDistributionsIDs;
	
	HashMap<String, Double> returnMap = new HashMap<String, Double>();


	private HashMap<String, List<BloomFilterI>> getBucketFromDatasets(List<String> distributions) {

		return new BucketDBHelper().getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES, distributions);

	}

	/* (non-Javadoc)
	 * @see lodVader.application.subsetdetection.SubsetDetectionI#setSourceDistribution(lodVader.mongodb.collections.DistributionDB)
	 */
	@Override
	public void setSourceDistribution(DistributionDB sourceDistribution) {
		this.sourceDistribution = sourceDistribution;		
	}

	/* (non-Javadoc)
	 * @see lodVader.application.subsetdetection.SubsetDetectionI#setTargetDistributions(java.util.List)
	 */
	@Override
	public void setTargetDistributions(List<String> targetDistributions) {
		this.targetDistributionsIDs = targetDistributions;
	}

	/* (non-Javadoc)
	 * @see lodVader.application.subsetdetection.SubsetDetectionI#getCounters(java.util.HashMap)
	 */
	@Override
	public HashMap<String, Double> getCounters(){
		return returnMap;
	}

	/* (non-Javadoc)
	 * @see lodVader.application.subsetdetection.SubsetDetectionI#detectSubsets()
	 */
	@Override
	public void detectSubsets() {
		// 3. load the buckets of the source and the target distriution
		HashMap<String, List<BloomFilterI>> distributionsBF = getBucketFromDatasets(targetDistributionsIDs);

		// get BF from the source distribution
		List<BloomFilterI> mainDistributionBFs = distributionsBF.get(sourceDistribution.getID());

		// 4. compare all bfs
		for (String targetDistribution : targetDistributionsIDs) {
			
			double commonTriples = 0.0;

			// iterate over all BF from the main distribution
			for (BloomFilterI mainBF : mainDistributionBFs) {

				for (BloomFilterI partialBF : distributionsBF.get(targetDistribution)) {
					commonTriples = commonTriples + mainBF.intersection(partialBF);
				}
			}
			if (commonTriples > 0.0){				
				returnMap.put(targetDistribution, commonTriples);
			}	
		}
	}

	// 4. compare all bfs

	// 5. if the size of the smaller dataset is the same number of the common
	// triples, its a subset (subset discovered)

	// 6. if both datasets have roughly the same size and roughly the same
	// number of triples, they are different versions.

	// save the common number of triples into database

}
