/**
 * 
 */
package lodVader.plugins.intersection.subset.linkset;

import java.util.List;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.plugins.intersection.subset.SubsetDetectionService;
import lodVader.services.mongodb.resourceRelation.GeneralResourceRelationServices;

/**
 * @author Ciro Baron Neto
 * 
 * Oct 14, 2016
 */
public class LinksetDetectionService extends SubsetDetectionService{

	 
	/**
	 * Constructor for Class DetectionService 
	 * @param detector
	 * @param distribution 
	 */
	public LinksetDetectionService(LODVaderIntersectionPlugin detector, DistributionDB distribution) {
		super(detector, distribution);
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<String> loadTargetDatasetsIds() {
			List<String> sourceNamespace = new GeneralResourceRelationServices().getSetOfResourcesID(distribution.getID(),
					COLLECTIONS.RELATION_OBJECT_NS);
			return new GeneralResourceRelationServices().getCommonDistributionsByResourceID(sourceNamespace,
					COLLECTIONS.RELATION_SUBJECT_NS);
	}
	
}
