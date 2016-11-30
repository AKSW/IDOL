/**
 * 
 */
package lodVader.plugins.intersection.subset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public abstract class SubsetDetectionService {

	LODVaderIntersectionPlugin subsetDetector;

	protected DistributionDB distribution;

	protected List<String> datasetsIDs = null;

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
		loadTargetDatasetsIds();
//		return subsetDetector.runDetection(distribution, datasetsIDs);
		return null;
	}

	public abstract List<String> loadTargetDatasetsIds();

	public void saveSubsets() {
		if (datasetsIDs == null) 
			datasetsIDs = loadTargetDatasetsIds();

		HashMap<String, Double> results = null;
//				subsetDetector.runDetection(distribution, datasetsIDs);

		List<DBObject> list = new ArrayList<>();

		for (String result : results.keySet()) {
			DBObject object = new BasicDBObject();
			object.put(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION, distribution.getID());
			object.put(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION, result);
			object.put(LODVaderIntersectionPlugin.VALUE, results.get(result).intValue());
//			object.put(LODVaderIntersectionPlugin.IMPLEMENTATION, subsetDetector.implementationName);
			list.add(object);
		}
		subsetDetector.getDB().bulkSave2(list);

	}

}
