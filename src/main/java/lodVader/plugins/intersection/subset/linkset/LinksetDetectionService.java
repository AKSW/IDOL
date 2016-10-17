/**
 * 
 */
package lodVader.plugins.intersection.subset.linkset;

import java.util.List;

import org.bson.types.ObjectId;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.plugins.intersection.subset.SubsetDetectionService;
import lodVader.services.mongodb.resource.GeneralResourceServices;
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
	}

	@Override
	public List<String> loadTargetDatasetsIds() {
		return null;
//			List<ObjectId> sourceNamespaceIDs = new GeneralResourceRelationServices().getSetOfResourcesID(distribution.getID(),
//					COLLECTIONS.RELATION_OBJECT_NS0);
//			
//			List<String>  sourceResourcesURL = new GeneralResourceServices().getSetOfResourcesURL(sourceNamespaceIDs, lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0);
//			
//			List<ObjectId>  targetNamespacesIDs = new GeneralResourceServices().getSetOfResourcesID(sourceResourcesURL, lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0);
//			
//			return new GeneralResourceRelationServices().getCommonDistributionsByResourceID(targetNamespacesIDs,
//					COLLECTIONS.RELATION_SUBJECT_NS0);
	}
	
}
