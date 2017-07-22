/**
 * 
 */
package org.aksw.idol.plugins.intersection.subset.linkset;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.idol.application.Manager;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import org.aksw.idol.services.mongodb.GeneralResourceRelationServices;
import org.aksw.idol.services.mongodb.GeneralResourceServices;
import org.aksw.idol.utils.NSUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

		List<String> sourceResourcesURL = filterNs( new GeneralResourceServices().getSetOfResourcesURL(sourceNamespaceIDs,
				org.aksw.idol.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0));

		List<String> targetNamespacesIDs = new GeneralResourceServices().getSetOfResourcesID(sourceResourcesURL,
				org.aksw.idol.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0);

		return new GeneralResourceRelationServices().getCommonDistributionsByResourceObjectID(targetNamespacesIDs,
				COLLECTIONS.RELATION_SUBJECT_NS0, minResources);
	}

	public List<String> loadIndegreeTargetDatasetsIds(DistributionDB distribution, int minResources) {
		logger.info("Loading indegree data for " + distribution.getDownloadUrl());

		List<ObjectId> sourceNamespaceIDs = new GeneralResourceRelationServices()
				.getSetOfResourcesID(distribution.getID(), COLLECTIONS.RELATION_SUBJECT_NS0);

		List<String> sourceResourcesURL = filterNs(new GeneralResourceServices().getSetOfResourcesURL(sourceNamespaceIDs,
				org.aksw.idol.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0));

		List<String> targetNamespacesIDs = new GeneralResourceServices().getSetOfResourcesID(sourceResourcesURL,
				org.aksw.idol.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_OBJECT_NS0);

		return new GeneralResourceRelationServices().getCommonDistributionsByResourceObjectID(targetNamespacesIDs,
				COLLECTIONS.RELATION_OBJECT_NS0, minResources);
	}

	private List<String> filterNs(List<String> l) {
		HashSet<String> h = new HashSet<>(l);
		NSUtils nsUtils = new NSUtils();
		for (String s : l) {
			String ns = nsUtils.getNS0(s);
			if (ns.contains("purl.org") || ns.contains("www.w3.org") || ns.contains("xmlns.com")) {
				h.remove(s);
			}
		}

		return new ArrayList<>(h);
	}

}
