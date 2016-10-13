/**
 * 
 */
package lodVader.services.intersection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBObjectCodec;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.services.mongodb.resourceRelation.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class SubsetDetectionService {

	LODVaderIntersectionPlugin subsetDetector;

	DistributionDB distribution;

	List<String> subjectNamespace = null;

	List<String> datasetsIDs = null;

	/**
	 * Constructor for Class SubsetDetectionService
	 */
	public SubsetDetectionService(LODVaderIntersectionPlugin detector, DistributionDB distribution) {
		this.subsetDetector = detector;
		this.distribution = distribution;
	}

	/**
	 * Find datasets suitable to be compared to (based on the subject
	 * namespaces), and run the detector
	 * 
	 * @return
	 */
	public HashMap<String, Double> runDetector() {
		loadNamespaces();
		// 3. run the detector
		return subsetDetector.detectSubsets(distribution, datasetsIDs);

	}

	public void loadNamespaces() {
		if (subjectNamespace == null)
			// 1. find ns described by both distributions
			subjectNamespace = new GeneralResourceRelationServices().getSetOfResourcesID(distribution.getID(),
					COLLECTIONS.RELATION_SUBJECT_NS);
		if (datasetsIDs == null)
			// 2. find distributions which describe the same NSs
			datasetsIDs = new GeneralResourceRelationServices().getCommonDistributionsByResourceID(subjectNamespace,
					COLLECTIONS.RELATION_SUBJECT_NS);
	}

	public void saveSubsets() {
		loadNamespaces();
		
		HashMap<String, Double> results = subsetDetector.detectSubsets(distribution, datasetsIDs);
		
		
		List<DBObject> list = new ArrayList<>();
		
		for(String result : results.keySet()){
			DBObject object = new BasicDBObject();
			object.put(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION,distribution.getID());
			object.put(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION,result);
			object.put(LODVaderIntersectionPlugin.VALUE,results.get(result).intValue());
			object.put(LODVaderIntersectionPlugin.IMPLEMENTATION, subsetDetector.implementationName);
			list.add(object);
		}
		subsetDetector.getDB().bulkSave2(list);
		
	}

}
