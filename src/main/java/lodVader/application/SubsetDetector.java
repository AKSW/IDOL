/**
 * 
 */
package lodVader.application;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB.COLLECTIONS;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.services.mongodb.CachedBucketService;
import lodVader.services.mongodb.GeneralResourceRelationServices;
import lodVader.services.mongodb.GeneralResourceServices;
import lodVader.utils.FileStatement;
import lodVader.utils.NSUtils;
import lodVader.utils.Timer;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 28, 2016
 */
public class SubsetDetector extends LODVaderIntersectionPlugin implements Runnable {

	final static Logger logger = LoggerFactory.getLogger(SubsetDetector.class);

	DistributionDB distribution;

	COLLECTIONS relationCollection = COLLECTIONS.RELATION_SUBJECT_NS0;
	lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS resourceCollection = lodVader.mongodb.collections.Resources.GeneralResourceDB.COLLECTIONS.RESOURCES_SUBJECT_NS0;

	// list of distributions per namespaces
	HashMap<String, List<String>> namespaceDistributionMap = new HashMap<>();

	NSUtils nsUtils = new NSUtils();

	// map of distributionID -> subsetExtractor
	HashMap<String, SubsetExtractor> extractorSet = new HashMap<>();

	
	int runExtractorEvery = 5_000;

	// executor service which will run the exctractors
	ExecutorService ex = null;

	DecimalFormat formatter = new DecimalFormat("###,###,###,###,###");

	/**
	 * Constructor for Class LODVader.DetectSubsets
	 */
	public SubsetDetector(DistributionDB distribution) {
		super("SUBSET_LD_LEX_IMPL");
		this.distribution = distribution;
	}

	@Override
	public void run() {
		logger.info("Datasets to be processed: " + LODVader.distributionsBeingProcessed.decrementAndGet());
		if (!shouldProcess())
			return;

		logger.info("Discovering subset for " + distribution.getDownloadUrl());

		/**
		 * Get all namespaces described by this distribution
		 */
		GeneralResourceRelationServices relationServices = new GeneralResourceRelationServices();
		List<String> resourcesID = relationServices.getSetOfResourcesIDAsString(distribution.getID(),
				relationCollection);
		logger.info(
				"The distribution " + distribution.getTitle() + " describes " + resourcesID.size() + " namespaces.");

		/**
		 * Get all NS resources (namespaces + namespacesids)
		 */
		GeneralResourceServices resourceServices = new GeneralResourceServices();
		HashMap<String, String> namespacesMap = resourceServices.getSetOfResourcesInstances(resourcesID,
				resourceCollection);

		/**
		 * Get target distributions which describe the same namespaces
		 */
		HashMap<String, List<String>> targetDistributionsNamespaces = relationServices
				.getCommonDistributionsByResourceID(resourcesID, relationCollection);
		// remove the target dist which has the same id of source dist
		targetDistributionsNamespaces.remove(distribution.getID());

		if (targetDistributionsNamespaces.size() == 0) {
			logger.info("0 distriutions overlaps namespaces with " + distribution.getTitle());
			return;
		} else
			logger.info("Another " + targetDistributionsNamespaces.size() + " distriution(s) overlap namespaces with "
					+ distribution.getTitle());
		logger.info("Distribution size: " + formatter.format(distribution.getNumberOfTriples()));

		if (targetDistributionsNamespaces.size() > 1000)
			return;

		/**
		 * Get filters for target distributions
		 */
		CachedBucketService bucketService = new CachedBucketService(
				lodVader.mongodb.collections.datasetBF.BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES);
		HashMap<String, BloomFilterI> filters = bucketService
				.loadBucketIntoCache(targetDistributionsNamespaces.keySet());
		logger.info(filters.size() + " filters lodaded. ");

		/**
		 * Create a map of namespaces -> distribution. E.g: http://dbpedia.org/
		 * -> [dbpediaDistribution1ID, dbpediaDistribution2ID, ...]
		 */
		for (String distribution : targetDistributionsNamespaces.keySet()) {
			for (String ns : targetDistributionsNamespaces.get(distribution)) {
				if (namespaceDistributionMap.get(namespacesMap.get(ns)) == null) {
					List<String> distributions = new ArrayList<>();
					distributions.add(distribution);
					namespaceDistributionMap.put(namespacesMap.get(ns), distributions);
				} else {
					namespaceDistributionMap.get(namespacesMap.get(ns)).add(distribution);
				}
			}
		}

		/**
		 * Create a map of extractors
		 */
		for (String distribution : targetDistributionsNamespaces.keySet()) {
			BloomFilterI bf = filters.get(distribution);
			SubsetExtractor extractor = new SubsetExtractor(bf);
			extractorSet.put(distribution, extractor);
		}

		/**
		 * Load all triples from the distribution using filestatement
		 */
		FileStatement triples = new FileStatement(LODVaderProperties.RAW_FILE_PATH + distribution.getID());
		int count = 0;
		int countTotal = 0;

		while (triples.hasNext()) {
			Statement st = triples.getStatement();
			String resource = st.getSubject().stringValue();
			String triple = st.getSubject().stringValue() + " " + st.getPredicate().stringValue() + " "
					+ st.getObject().stringValue();

			// for each resource, get the namespace
			String ns = nsUtils.getNS0(resource);
			countTotal++;

			if (!ns.equals("")) {

				// check all distribution which describe this particular
				// namespace
				if (namespaceDistributionMap.get(ns) != null) {
					for (String dist : namespaceDistributionMap.get(ns)) {
						// add the resource to the correct distribution
						// extractor

						extractorSet.get(dist).addResource(triple);

					}
				}

				// run extractors every 20.000 iterations
				if (++count % runExtractorEvery == 0) {
					logger.info("Running extractors... (" + formatter.format(count) + ") triples, "
							+ formatter.format((distribution.getNumberOfTriples() - countTotal)) + " remaining.");

					runExtractor();
				}
			}

		}

		runExtractor();

		// return results
		logger.info("Returning subsets data...");

		HashMap<String, Long> r = new HashMap<>();
		for (String dist : extractorSet.keySet()) {
			if (extractorSet.get(dist).r > 500)
				r.put(dist, extractorSet.get(dist).r);
		}
		save(r, distribution.getID());

	}

	private boolean shouldProcess() {

		GeneralQueriesHelper q = new GeneralQueriesHelper();
		if (q.getObjects("PLUGIN_" + getPluginName(), LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION,
				distribution.getID()).iterator().hasNext()) {
			return false;
		}
		if (distribution.getNumberOfTriples() < 1000)
			return false;
		if (distribution.getDownloadUrl().contains("dbpedia"))
			return false;
		if (distribution.getUri().contains("dbpedia"))
			return false;
		if(distribution.getNumberOfTriples() > 50_000_000)
			return false;

		return true;
	}

	private void runExtractor() {

		Timer t = new Timer();
		t.startTimer();
		ex = Executors.newFixedThreadPool(8);
		for (String dist : extractorSet.keySet()) {
			ex.submit(extractorSet.get(dist));
		}

		ex.shutdown();

		try {
			ex.awaitTermination(500, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info("Extractor timer: " + t.stopTimer());

	}

	class SubsetExtractor implements Runnable {

		HashSet<String> resources = new HashSet<>();
		BloomFilterI bf;
		long r = 0;

		public long extractedCounter() {
			return r;
		}

		public void addResource(String resource) {
			resources.add(resource);
		}

		public SubsetExtractor(BloomFilterI bf) {
			super();
			this.bf = bf;
		}

		public void run() {
			for (String resource : resources) {
				if (bf.compare(resource))
					r++;
			}
			resources = null;
			resources = new HashSet<>();
		}

	}

}