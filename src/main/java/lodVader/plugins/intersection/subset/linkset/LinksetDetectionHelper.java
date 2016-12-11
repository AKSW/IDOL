/**
 * 
 */
package lodVader.plugins.intersection.subset.linkset;

import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.application.LODVader;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.services.mongodb.GeneralResourceRelationServices;
import lodVader.services.mongodb.GeneralResourceServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 14, 2016
 */
public class LinksetDetectionHelper {

	final static Logger logger = LoggerFactory.getLogger(LinksetDetectionHelper.class);

	public List<String> loadOutdegreeTargetDatasetsIds(DistributionDB distribution, int minResources) {
		logger.info("Loading outdegree data for " + distribution.getDownloadUrl());
		
		List<ObjectId> sourceNamespaceIDs = new GeneralResourceRelationServices()
				.getSetOfResourcesID(distribution.getID(), COLLECTIONS.RELATION_OBJECT_NS0);

		List<String> sourceResourcesURL = new GeneralResourceServices().getSetOfResourcesURL(sourceNamespaceIDs,
				lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0);


		List<String> targetNamespacesIDs = new GeneralResourceServices().getSetOfResourcesID(sourceResourcesURL,
				lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0);

		return new GeneralResourceRelationServices().getCommonDistributionsByResourceObjectID(targetNamespacesIDs,
				COLLECTIONS.RELATION_SUBJECT_NS0, minResources);
	}

	public List<String> loadIndegreeTargetDatasetsIds(DistributionDB distribution, int minResources) {
		logger.info("Loading indegree data for " + distribution.getDownloadUrl());
		
		List<ObjectId> sourceNamespaceIDs = new GeneralResourceRelationServices()
				.getSetOfResourcesID(distribution.getID(), COLLECTIONS.RELATION_SUBJECT_NS0);

		List<String> sourceResourcesURL = new GeneralResourceServices().getSetOfResourcesURL(sourceNamespaceIDs,
				lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0);

		

		List<String> targetNamespacesIDs = new GeneralResourceServices().getSetOfResourcesID(sourceResourcesURL,
				lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0);

		return new GeneralResourceRelationServices().getCommonDistributionsByResourceObjectID(targetNamespacesIDs,
				COLLECTIONS.RELATION_OBJECT_NS0, minResources);
	}

}
