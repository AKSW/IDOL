/**
 * 
 */
package lodVader.plugins.intersection.subset.linkset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.mongodb.collections.datasetBF.BucketService;
import lodVader.plugins.LODVaderPlugin;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.services.mongodb.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 11, 2016
 */
public class LinksetDetectorBFImpl extends LODVaderIntersectionPlugin{

	
	public static String PLUGIN_NAME = "LINKSET_BLOOM_FILTER_DETECTOR";
	
	/**
	 * Constructor for Class SubsetDetectorBFImpl 
	 * @param pluginName
	 */
	public LinksetDetectorBFImpl() {
		super(PLUGIN_NAME);
	}

	private HashMap<String, List<BloomFilterI>> getBucketFromDatasets(List<String> distributions) {

		return new BucketService().getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_SUBJECTS, distributions);

	}
	
	private List<BloomFilterI> getBucketFromMainDistribution(String distribution) {

		List<String> list = new ArrayList<String>();
		list.add(distribution);
		HashMap<String, List<BloomFilterI>> filters = new BucketService().getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_OBJECTS, list);
 		if(filters.size()>0){
 			return filters.values().iterator().next();
 		}
 		else return null;

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
		List<BloomFilterI> mainDistributionBFs = getBucketFromMainDistribution(sourceDistribution.getID());

		// compare all bfs
		if(mainDistributionBFs != null)
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
