/**
 * 
 */
package lodVader.API.application.subsetdetection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.GeneralResourceRelationDB;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.mongodb.collections.datasetBF.BucketDBHelper;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 9, 2016
 */
public class SubsetDetection {

	DistributionDB distributionDB;

	/**
	 * Constructor for Class SubsetDetection
	 */
	public SubsetDetection(DistributionDB distributionDB) {
		this.distributionDB = distributionDB;
	}

	public void detectSubsets() {

		// 1. find ns described by both distributions
		List<String> subjectNamespace = getDistributionNS();

		// 2. find distributions which describe the same NSs
		List<String> datasetsID = getCommonDistributionsByNS(subjectNamespace);

		// 3. load the buckets of the source and the target distriution
		HashMap<String, List<BloomFilterI>> distributionsBF = getBucketFromDatasets(datasetsID);

		// get BF from the source distribution
		List<BloomFilterI> mainDistributionBFs = distributionsBF.get(distributionDB.getID());

		// 4. compare all bfs
		for (String distribution : distributionsBF.keySet()) {
			
			double commonTriples = 0.0;

			// iterate over all BF from the main distribution
			for (BloomFilterI mainBF : mainDistributionBFs) {

				for (BloomFilterI partialBF : distributionsBF.get(distribution)) {
					commonTriples = commonTriples + mainBF.intersection(partialBF);
				}
			}
			
			if (commonTriples > 0.0){
				// save to mongodb
				
				System.out.println(commonTriples);
			}
			
		}
	}

	private List<String> getDistributionNS() {

		List<String> subjectNamespacesID = new ArrayList<>();

		// get namespaces from the source datasets
		BasicDBObject query = new BasicDBObject(GeneralResourceRelationDB.DISTRIBUTION_ID, distributionDB.getID());
		GeneralResourceRelationDB.getCollection(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS.toString())
				.find(query).forEach((object) -> {
					subjectNamespacesID.add(object.get(GeneralResourceRelationDB.PREDICATE_ID).toString());
				});

		return subjectNamespacesID;
	}

	private List<String> getCommonDistributionsByNS(List<String> namespaces) {

		List<String> datasetsID = new ArrayList<>();

		// get datasets which describe common namespaces (target datasets)
		BasicDBObject query = new BasicDBObject(GeneralResourceRelationDB.PREDICATE_ID,
				new BasicDBObject("$in", namespaces));
		GeneralResourceRelationDB.getCollection(GeneralResourceRelationDB.COLLECTIONS.RELATION_SUBJECT_NS.toString())
				.find(query).forEach((object) -> {
					datasetsID.add(object.get(GeneralResourceRelationDB.DISTRIBUTION_ID).toString());
				});

		return datasetsID;
	}

	private HashMap<String, List<BloomFilterI>> getBucketFromDatasets(List<String> distributions) {

		return new BucketDBHelper().getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES, distributions);

	}

	// 4. compare all bfs

	// 5. if the size of the smaller dataset is the same number of the common
	// triples, its a subset (subset discovered)

	// 6. if both datasets have roughly the same size and roughly the same
	// number of triples, they are different versions.

	// save the common number of triples into database

}
