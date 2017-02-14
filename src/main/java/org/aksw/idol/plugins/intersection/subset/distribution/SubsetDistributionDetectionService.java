/**
 * 
 */
package org.aksw.idol.plugins.intersection.subset.distribution;

import java.util.List;

import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import org.aksw.idol.plugins.intersection.LODVaderIntersectionPlugin;
import org.aksw.idol.plugins.intersection.subset.SubsetDetectionService;
import org.aksw.idol.services.mongodb.GeneralResourceRelationServices;
import org.aksw.idol.services.mongodb.GeneralResourceServices;
import org.bson.types.ObjectId;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class SubsetDistributionDetectionService extends SubsetDetectionService {

	/**
	 * Constructor for Class SubsetDistributionDetectionService
	 * 
	 * @param detector
	 * @param distribution
	 */
	public SubsetDistributionDetectionService(LODVaderIntersectionPlugin detector, DistributionDB distribution) {
		super(detector, distribution);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see idol.plugins.intersection.subset.SubsetDetectionService#
	 * loadTargetDatasetsIds()
	 */
	@Override
	public List<String> loadTargetDatasetsIds() {

		List<String> sourceNamespacesIDs = new GeneralResourceRelationServices()
				.getSetOfResourcesIDAsString(distribution.getID(), COLLECTIONS.RELATION_SUBJECT_NS0);
		
		//TODO Fix here
//		return (List<String>) new GeneralResourceRelationServices().getCommonDistributionsByResourceID(sourceNamespacesIDs,
//				COLLECTIONS.RELATION_SUBJECT_NS0);
		
		return null;
	}

}
