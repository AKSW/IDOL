/**
 * 
 */
package lodVader.application.fileparser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.adapters.DatasetDBAdapter;
import lodVader.mongodb.collections.adapters.DistributionDBAdapter;
import lodVader.mongodb.collections.ckanparser.CkanDatasetDB;
import lodVader.mongodb.collections.ckanparser.CkanResourceDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.parsers.descriptionFileParser.helpers.SubsetHelper;
import lodVader.utils.FormatsUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 28, 2016
 */
public class CkanToLODVaderConverter {

	public void convert(String dataSourceName) {

		List<DistributionDB> distributions = new ArrayList<>();
		HashMap<String, DatasetDB> datasets = new HashMap<>();

		new GeneralQueriesHelper().getObjects(CkanResourceDB.COLLECTION_NAME, new BasicDBObject()).forEach((obj) -> {
			CkanResourceDB ckanResourceDB = new CkanResourceDB(obj);
			if (ckanResourceDB.getFormat() != null)
				if (!FormatsUtils.getEquivalentFormat(ckanResourceDB.getFormat()).equals("")) {
					DistributionDB distributionDB = new DistributionDBAdapter(ckanResourceDB, dataSourceName);
					distributionDB.find(true, distributionDB.DOWNLOAD_URL, distributionDB.getDownloadUrl());

					CkanDatasetDB ckanDatasetDB = new CkanDatasetDB();
					ckanDatasetDB.find(true, CkanDatasetDB.CKAN_ID, ckanResourceDB.getCkanDataset());

					DatasetDB datasetDB = new DatasetDBAdapter(ckanDatasetDB, dataSourceName);
					datasetDB.find(true, DatasetDB.URI, datasetDB.getUri());
					
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
					
//					distributions.add(distributionDB);
//					datasets.put(datasetDB.getID(), datasetDB);
					
				}
		});
		new SubsetHelper().rearrangeSubsets(distributions, datasets);

	}

}
