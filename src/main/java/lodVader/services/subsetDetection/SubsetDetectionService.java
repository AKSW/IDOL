/**
 * 
 */
package lodVader.services.subsetDetection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.services.mongodb.resourceRelation.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 11, 2016
 */
public class SubsetDetectionService {

	SubsetDetectorI subsetDetector;

	DistributionDB distribution;
	
	/**
	 * Constructor for Class SubsetDetectionService 
	 */
	public SubsetDetectionService(SubsetDetectorI detector, DistributionDB distribution) {
		this.subsetDetector = detector;
		this.distribution = distribution;
	}
	
	public HashMap<String, Double> runDetector(){
		// 1. find ns described by both distributions
		List<String> subjectNamespace = new GeneralResourceRelationServices().getSetOfResourcesID(distribution.getID(), COLLECTIONS.RELATION_SUBJECT_NS);

		// 2. find distributions which describe the same NSs
		List<String> datasetsIDs =  new GeneralResourceRelationServices().getCommonDistributionsByResourceID(subjectNamespace, COLLECTIONS.RELATION_SUBJECT_NS);;

		subsetDetector.setSourceDistribution(distribution);
		subsetDetector.setTargetDistributions(datasetsIDs);
		
		subsetDetector.detectSubsets();
		
		return subsetDetector.getCounters();		
	}
	
}
