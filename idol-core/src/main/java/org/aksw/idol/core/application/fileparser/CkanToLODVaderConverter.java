/**
 * 
 */
package org.aksw.idol.core.application.fileparser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.aksw.idol.core.mongodb.collections.DatasetDB;
import org.aksw.idol.core.mongodb.collections.DistributionDB;
import org.aksw.idol.core.mongodb.collections.adapters.DatasetDBAdapter;
import org.aksw.idol.core.mongodb.collections.adapters.DistributionDBAdapter;
import org.aksw.idol.core.mongodb.collections.ckanparser.CkanDatasetDB;
import org.aksw.idol.core.mongodb.collections.ckanparser.CkanResourceDB;
import org.aksw.idol.core.mongodb.queries.GeneralQueriesHelper;
import org.aksw.idol.core.parsers.descriptionFileParser.helpers.SubsetHelper;
import org.aksw.idol.utils.FormatsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 28, 2016
 */
public class CkanToLODVaderConverter {

	final static Logger logger = LoggerFactory.getLogger(CkanToLODVaderConverter.class);

	public void convert(String dataSourceName) {

		List<DistributionDB> distributions = new ArrayList<>();
		HashMap<String, DatasetDB> datasets = new HashMap<>();

		logger.info("Converting resources from " + CkanResourceDB.COLLECTION_NAME);
		new GeneralQueriesHelper().getObjects(CkanResourceDB.COLLECTION_NAME,
				new BasicDBObject(CkanResourceDB.DATASOURCE, dataSourceName)).forEach((obj) -> {
					CkanResourceDB ckanResourceDB = new CkanResourceDB(obj);
					if (ckanResourceDB.getFormat() != null)
						if (!FormatsUtils.getSerializationFormat(ckanResourceDB.getFormat()).equals("")) {
							DistributionDB distributionDB = new DistributionDBAdapter(ckanResourceDB, dataSourceName);
							distributionDB.find(true, distributionDB.DOWNLOAD_URL, distributionDB.getDownloadUrl());
							distributionDB.addDatasource(dataSourceName);

							CkanDatasetDB ckanDatasetDB = new CkanDatasetDB();
							ckanDatasetDB.find(true, CkanDatasetDB.CKAN_ID, ckanResourceDB.getCkanDataset());

							DatasetDB datasetDB = new DatasetDBAdapter(ckanDatasetDB, dataSourceName);
							datasetDB.find(true, DatasetDB.URI, datasetDB.getUri());
							datasetDB.addProvenance(dataSourceName);

							try {
								datasetDB.update();
								distributionDB.update();
							} catch (Exception e) {
								e.printStackTrace();
							}

							distributionDB.setTopDataset(datasetDB.getID());
							datasetDB.addDistributionID(distributionDB.getID());

							try {
								datasetDB.update();
								distributionDB.update();
							} catch (Exception e) {
								e.printStackTrace();
							}

							// distributions.add(distributionDB);
							// datasets.put(datasetDB.getID(), datasetDB);

						}
				});
		logger.info("Ckan datasets converted. ");
		logger.info("Rearranging subsets...");

		new SubsetHelper().rearrangeSubsets(distributions, datasets);
		logger.info("Subsets rearranged.");

	}

}
