package org.aksw.idol.core.API.diagram;

import java.util.HashMap;
import java.util.HashSet;

import org.aksw.idol.core.mongodb.collections.DistributionDB;

public class DiagramData {
	
	// load all distributions
	public HashSet<Integer> distributionsID = new HashSet<Integer>();
	
	public HashMap<Integer, DistributionDB> loadedDistributions = new HashMap<Integer, DistributionDB>();

//	public HashMap<Integer, ArrayList<LinksetDB>> indegreeLinks = new HashMap<Integer, ArrayList<LinksetDB>>();
//	public HashMap<Integer, ArrayList<LinksetDB>> outdegreeLinks = new HashMap<Integer, ArrayList<LinksetDB>>();

}
