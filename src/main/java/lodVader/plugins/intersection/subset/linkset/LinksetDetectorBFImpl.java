/**
 * 
 */
package lodVader.plugins.intersection.subset.linkset;

import java.util.HashMap;
import java.util.List;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.mongodb.collections.datasetBF.BucketDBHelper;
import lodVader.plugins.LODVaderPlugin;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.services.mongodb.resourceRelation.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 11, 2016
 */
public class LinksetDetectorBFImpl extends LODVaderIntersectionPlugin{

	
	public static String PLUGIN_NAME = "SUBSET_BLOOM_FILTER_DETECTOR";
	
	/**
	 * Constructor for Class SubsetDetectorBFImpl 
	 * @param pluginName
	 */
	public LinksetDetectorBFImpl() {
		super(PLUGIN_NAME);
	}

	private HashMap<String, List<BloomFilterI>> getBucketFromDatasets(List<String> distributions) {

		return new BucketDBHelper().getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_SUBJECTS, distributions);

	}

	/* (non-Javadoc)
	 * @see lodVader.application.subsetdetection.SubsetDetectionI#detectSubsets()
	 */
	@Override
	public HashMap<String, Double> runDetection(DistributionDB sourceDistribution, List<String> targetDistributionsIDs) {
		
		HashMap<String, Double> returnMap = new HashMap<String, Double>();
		
		// load the buckets of the source and the target distriution
		HashMap<String, List<BloomFilterI>> distributionsBF = getBucketFromDatasets(targetDistributionsIDs);

		// get BF from the source distribution
		List<BloomFilterI> mainDistributionBFs = distributionsBF.get(sourceDistribution.getID());

		// compare all bfs
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
		
		return returnMap;
	}

}
