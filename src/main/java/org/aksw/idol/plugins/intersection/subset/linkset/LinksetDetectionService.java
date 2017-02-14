/**
 * 
 */
package org.aksw.idol.plugins.intersection.subset.linkset;

import java.util.List;

import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import org.aksw.idol.plugins.intersection.LODVaderIntersectionPlugin;
import org.aksw.idol.plugins.intersection.subset.SubsetDetectionService;
import org.aksw.idol.services.mongodb.GeneralResourceRelationServices;
import org.aksw.idol.services.mongodb.GeneralResourceServices;
import org.bson.types.ObjectId;

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
//			List<String>  sourceResourcesURL = new GeneralResourceServices().getSetOfResourcesURL(sourceNamespaceIDs, idol.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0);
//			
//			List<ObjectId>  targetNamespacesIDs = new GeneralResourceServices().getSetOfResourcesID(sourceResourcesURL, idol.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0);
//			
//			return new GeneralResourceRelationServices().getCommonDistributionsByResourceID(targetNamespacesIDs,
//					COLLECTIONS.RELATION_SUBJECT_NS0);
	}
	
}
