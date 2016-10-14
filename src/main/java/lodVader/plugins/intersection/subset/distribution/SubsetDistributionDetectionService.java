/**
 * 
 */
package lodVader.plugins.intersection.subset.distribution;

import java.util.List;

import org.bson.types.ObjectId;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.plugins.intersection.subset.SubsetDetectionService;
import lodVader.services.mongodb.resourceRelation.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class SubsetDistributionDetectionService extends SubsetDetectionService{

	/**
	 * Constructor for Class SubsetDistributionDetectionService 
	 * @param detector
	 * @param distribution
	 */
	public SubsetDistributionDetectionService(LODVaderIntersectionPlugin detector, DistributionDB distribution) {
		super(detector, distribution);
	}



	/* (non-Javadoc)
	 * @see lodVader.plugins.intersection.subset.SubsetDetectionService#loadTargetDatasetsIds()
	 */
	@Override
	public List<String> loadTargetDatasetsIds() {
		List<ObjectId> sourceNamespace = new GeneralResourceRelationServices().getSetOfResourcesID(distribution.getID(),
				COLLECTIONS.RELATION_SUBJECT_NS0);
		return new GeneralResourceRelationServices().getCommonDistributionsByResourceID(sourceNamespace,
				COLLECTIONS.RELATION_SUBJECT_NS0);
	}


}
