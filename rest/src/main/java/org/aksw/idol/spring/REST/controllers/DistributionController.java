/**
 * 
 */
package org.aksw.idol.spring.REST.controllers;

import java.text.DecimalFormat;
import java.util.ArrayList;

import org.aksw.idol.core.mongodb.collections.DistributionDB;
import org.aksw.idol.core.mongodb.queries.DistributionQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 8, 2016
 */

public class DistributionController {

	final static Logger logger = LoggerFactory.getLogger(DistributionController.class);

	public final String rdfPath = "/distribution/compare/rdf";

	/**
	 * Return a distribution
	 * 
	 * @param id
	 * @return the distribution
	 */
	// @RequestMapping(value = "/distribution/{id}", method = RequestMethod.GET)
	// public DistributionDB dataset(@PathVariable String id) {
	public DistributionDB dataset(String id) {

		return new DistributionDB(id);
	}

	// @RequestMapping(value = "/distribution/list")
	// public ArrayList<DistributionDB> list() {
	public ArrayList<DistributionDB> list() {
		return new DistributionQueries().getDistributions(null, null, null);
	}

	// @RequestMapping(value = rdfPath, produces = "application/json;
	// charset=utf-8")
	// public String retrieveRDF(@RequestParam(value = "source", required =
	// false) String source,
	// @RequestParam(value = "target", required = false) String target,
	// HttpServletRequest request) {
	//
	// RetrieveRDF apiRetrieve = null;
	// StringWriter out = new StringWriter();
	//
	// try {
	// if (source != null && target != null) {
	// apiRetrieve = new RetrieveRDF(source, target,
	// request.getRequestURL().toString());
	// } else
	// apiRetrieve = new RetrieveRDF(source,
	// request.getRequestURL().toString());
	// apiRetrieve.outModel.write(out, "TURTLE");
	//
	// } catch (LODVaderNoDatasetFoundException e) {
	// throw new IllegalArgumentException(e);
	// } catch (DynamicLODAPINoLinksFoundException e) {
	// e.printStackTrace();
	// }
	// return out.toString();
	// }

	// @RequestMapping(value = "/distribution/size")
	// public String size(@RequestParam(value = "searchStatus", required =
	// false, defaultValue = "DONE") String status,
	// @RequestParam(value = "searchVocabularies", required = false) Boolean
	// vocabularies) {
	public String size(String status, Boolean vocabularies) {
		DecimalFormat format = new DecimalFormat("###,###,###,###");
		return format.format(new DistributionQueries().getDistributions(vocabularies, status, null).size());
	}

	// @RequestMapping(value = "/distribution/detail")
	// public DetailModel detail(@RequestParam(value = "distribution", required
	// = true) int distributionID,
	// @RequestParam(value = "topN", required = true) int topN,
	// @RequestParam(value = "type", required = true) String type,
	// HttpServletRequest request) {
	//
	// DetailModel detailModel = new DetailModel();
	// detailModel.details(distributionID, topN, type);
	//
	// detailModel.setRdfURL(request.getRequestURL().toString().replace("/distribution/detail",
	// "") + rdfPath
	// + "/?source=" +
	// detailModel.getDistribution().getDownloadUrl().replace("#", "%23"));
	//
	// return detailModel;
	// }

	// @RequestMapping(value = "/distribution/compare/list")
	// public CompareListModel compareList(@RequestParam(value = "distribution",
	// required = true) int distribution,
	// @RequestParam(value = "topN", required = true) int topN,
	// @RequestParam(value = "type", required = true) String type) {
	//
	// CompareListModel list = new CompareListModel();
	// list.makeCompareList(distribution, topN, type);
	//
	// return list;
	// }

	// @RequestMapping(value = "/distribution/compare/similarity/list")
	// public CompareSimilarityListModel compareSimilarityList(
	// @RequestParam(value = "distribution1", required = true) int
	// distribution1,
	// @RequestParam(value = "distribution2", required = true) int
	// distribution2,
	// @RequestParam(value = "topN", required = true) int topN,
	// @RequestParam(value = "type", required = true) String type) {
	//
	// CompareSimilarityListModel similarity = new CompareSimilarityListModel();
	// similarity.compareDatasets(distribution1, distribution2, type);
	//
	// return similarity;
	// }
	//
	// @RequestMapping(value = "/distribution/compare/topN")
	// public CompareTopNModel compareTopN(@RequestParam(value =
	// "distribution1", required = true) int distribution1,
	// @RequestParam(value = "distribution2", required = true) int
	// distribution2,
	// @RequestParam(value = "type", required = true) String type) {
	//
	// CompareTopNModel compare = new CompareTopNModel();
	// compare.getTopNLinks(distribution1, distribution2, type);
	//
	// return compare;
	// }

	// @RequestMapping(value = "/distribution/triples/size")
	// public String triplesSize(@RequestParam(value = "isVocab", required =
	// false) Boolean isVocab) {
	public String triplesSize(Boolean isVocab) {
		DecimalFormat format = new DecimalFormat("###,###,###,###");
		// if (isVocab == null)
		return format.format(new DistributionQueries().getNumberOfTriples());
		// else
		// return format.format(new
		// DistributionQueries().getNumberOfTriples(isVocab));
	}

	// @RequestMapping(value = "/distribution/search")
	// public StatusPageModel search(
	// @RequestParam(value = "search[value]", required = false, defaultValue =
	// "") String searchValue,
	// @RequestParam(value = "searchVocabularies", required = false) Boolean
	// searchVocabularies,
	// @RequestParam(value = "searchStatus", required = false, defaultValue =
	// "DONE") String searchStatus,
	// @RequestParam(value = "start", required = false, defaultValue = "0") int
	// start,
	// @RequestParam(value = "length", required = false, defaultValue = "5") int
	// length,
	// @RequestParam(value = "searchSubject", required = false, defaultValue =
	// "") String searchSubject,
	// @RequestParam(value = "searchProperty", required = false, defaultValue =
	// "") String searchProperty,
	// @RequestParam(value = "searchObject", required = false, defaultValue =
	// "") String searchObject) {
	//
	// StatusPageModel model = new StatusPageModel();
	// model.search(start, length, searchVocabularies, searchValue,
	// searchSubject, searchProperty, searchObject,
	// searchStatus);
	//
	// return model;
	// }

}
