package lodVader.spring.REST.controllers;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.spring.REST.models.DataSourceSizeRESTModel;
import lodVader.spring.REST.models.StreamingStatusRESTModel;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 8, 2016
 */

@RestController
public class ResultsController {

	final static Logger logger = LoggerFactory.getLogger(ResultsController.class);

	@RequestMapping(value = "/results/descriptionFile/subsets", method = RequestMethod.GET)
	public HashMap<String, Integer> descriptionFileSubsets() {

		HashMap<String, Integer> map = new HashMap<>();

		logger.info("Counting description file subsets...");

		for (DBObject d : new GeneralQueriesHelper().getObjects(DatasetDB.COLLECTION_NAME, new BasicDBObject())) {

			DatasetDB datasetDB = new DatasetDB(d);
			if (datasetDB.getDistributionsIDs() != null)
				if (datasetDB.getDistributionsIDs().size() > 1) {
					// if(datasetDB.getProvenance().equals("http://data.dws.informatik.uni-mannheim.de/lodcloud/2014/ISWC-RDB/datacatalog_metadata.tar.gz"))
					System.out.println(datasetDB.getProvenance() + datasetDB.getDistributionsIDs().size());
					if (map.get(datasetDB.getProvenance()) == null) {
						map.put(datasetDB.getProvenance(), datasetDB.getDistributionsIDs().size());
					} else {
						map.put(datasetDB.getProvenance(),
								map.get(datasetDB.getProvenance()) + datasetDB.getDistributionsIDs().size());

					}
				}

		}
		return map;

	}

	@RequestMapping(value = "/results/subsets", method = RequestMethod.GET)
	public HashMap<Double, Integer> dataset() {

		int value = 1;
		HashMap<Double, Integer> map = new HashMap<Double, Integer>();

		for (DBObject d : new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject(DistributionDB.STATUS, DistributionDB.DistributionStatus.DONE.toString()))) {
			DistributionDB distribution = new DistributionDB(d);

			List<DBObject> or = new ArrayList<>();
			or.add(new BasicDBObject(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString(),
					distribution.getID().toString()));
			or.add(new BasicDBObject(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION.toString(),
					distribution.getID().toString()));

			DBObject query = new BasicDBObject(new BasicDBObject("$or", or));

			ArrayList<DBObject> result = new GeneralQueriesHelper().getObjects("PLUGIN_INTERSECTION_PLUGIN", query, 1,
					LODVaderIntersectionPlugin.VALUE, -1);
			if (!result.isEmpty()) {
				DecimalFormat df = new DecimalFormat("#.#");
				double q = Double.valueOf(df
						.format(((Number) result.iterator().next().get(LODVaderIntersectionPlugin.VALUE)).doubleValue()
								/ distribution.getNumberOfTriples()));

				if (map.get(q) == null)
					map.put(q, value);
				else {
					map.put(q, map.get(q) + 1);
				}
			}

		}

		return map;
	}

	@RequestMapping(value = "/results/triplesInterval", method = RequestMethod.GET)
	public HashMap<Integer, Integer> triplesInterval(
			@RequestParam(value = "interval", required = false, defaultValue = "1000000") Integer interval,
			@RequestParam(value = "min", required = false, defaultValue = "0") Integer min
			) {
		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
		for (DBObject d : new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject(DistributionDB.STATUS, DistributionDB.DistributionStatus.DONE.toString()))) {
			DistributionDB distribution = new DistributionDB(d);
			if (distribution.getNumberOfTriples() > min) {
				int n = ((Number) (distribution.getNumberOfTriples() / interval)).intValue();
				if (map.get(n) == null) {
					map.put(n, 1);
				} else {
					map.put(n, map.get(n) + 1);
				}
			}
		}
		return map;
	}

	/**
	 * Returns a JSON object with the status of each datasource
	 * 
	 * @return
	 */
	@RequestMapping(value = "/results/streamingStatus", method = RequestMethod.GET)
	public StreamingStatusRESTModel streamingStatus() {
		StreamingStatusRESTModel model = new StreamingStatusRESTModel();
		model.checkStatus();
		return model;

	}

	/**
	 * Returns a JSON object with the uncompressed size of each datasource
	 * 
	 * @return
	 */
	@RequestMapping(value = "/results/datasourceSize", method = RequestMethod.GET)
	public DataSourceSizeRESTModel dasourceSize() {
		DataSourceSizeRESTModel model = new DataSourceSizeRESTModel();
		model.checkStatus();
		return model;

	}

	@RequestMapping(value = "/results/topPredicates", method = RequestMethod.GET)
	public HashMap<String, Integer> predicates() {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (DBObject d : new GeneralQueriesHelper().getObjects(
				GeneralResourceRelationDB.COLLECTIONS.RELATION_ALL_PREDICATES.toString(),
				new BasicDBObject(DistributionDB.STATUS, DistributionDB.DistributionStatus.DONE.toString()))) {
			if (map.get(d.get(GeneralResourceRelationDB.PREDICATE_ID).toString()) == null) {
				map.put(d.get(GeneralResourceRelationDB.PREDICATE_ID).toString(), 1);
			} else {
				map.put(d.get(GeneralResourceRelationDB.PREDICATE_ID).toString(),
						map.get(d.get(GeneralResourceRelationDB.PREDICATE_ID)) + 1);
			}
		}

		HashMap<ObjectId, Integer> returnMap = new HashMap<ObjectId, Integer>();
		for (String s : map.keySet()) {
			if (map.get(s) > 100) {
				returnMap.put(new ObjectId(s), map.get(s));
			}
		}

		// load predicates
		DBObject in = new BasicDBObject();
		in.put("$in", returnMap.keySet());

		List<DBObject> returnList = new GeneralQueriesHelper().getObjects(
				GeneralResourceDB.COLLECTIONS.RESOURCES_ALL_PREDICATES.toString(),
				new BasicDBObject(GeneralResourceDB.ID, in));

		HashMap<String, Integer> result = new HashMap<String, Integer>();

		for (DBObject o : returnList) {
			result.put(o.get(GeneralResourceDB.URI).toString(),
					map.get(new ObjectId(o.get(GeneralResourceDB.ID).toString()).toString()));
		}

		return result;
	}
}
