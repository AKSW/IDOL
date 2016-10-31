/**
 * 
 */
package lodVader.services.mongodb.distribution;

import java.util.HashSet;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;

/**
 * Some useful methods to handle distributions on MongoDB database.
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 11, 2016
 */
public class DistributionServices {

	/**
	 * Save an array of distributions which came from the same repository and
	 * datasource into MongoDB database
	 * 
	 * @param distributions
	 *            The list of distributions
	 * @param repository
	 *            The repository that the distributions came from
	 * @param datasource
	 *            The datasource that the distributions came from
	 */
	public void saveAllDistributions(List<DistributionDB> distributions, String repository, String datasource) {
		distributions.forEach((distribution) -> {
			try {
				distribution.addDatasource(datasource);
				distribution.addRepository(repository);
				distribution.update();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	/**
	 * Remove a distribution from MongoDB.
	 * 
	 * @param distribution
	 *            the distribution to be removed
	 * @param removeDataset
	 *            true case the dataset related with the distribution should be
	 *            deleted or not. Note: If the dataset contains more
	 *            distributions it will not delete the document, however, only
	 *            delete the relation.
	 */
	public void removeDistribution(DistributionDB distributionDB, boolean removeDataset) {

		// remove the distribution
		DBSuperClass.getCollection(DistributionDB.COLLECTION_NAME)
				.remove(new BasicDBObject(DistributionDB.DOWNLOAD_URL, distributionDB.getDownloadUrl()));

		// iterate over all datasets and remove the relation between the dataset
		// and distribution.
		if (removeDataset) {
			List<DBObject> objects = new GeneralQueriesHelper().getObjects(DatasetDB.COLLECTION_NAME,
					DatasetDB.DISTRIBUTIONS_IDS, distributionDB.getID());

			for (DBObject object : objects) {
				DatasetDB datasetDB = new DatasetDB(object);
				HashSet<String> distributionIDs = new HashSet<>(datasetDB.getDistributionsIDs());
				distributionIDs.remove(distributionDB.getID());

				// if the dataset doesn't have any connections, remove it
				if (distributionIDs.size() == 0)
					DBSuperClass.getCollection(DatasetDB.COLLECTION_NAME)
							.remove(new BasicDBObject(DatasetDB.ID, datasetDB.getID()));

				// else just save the updated list of relations
				else {
					datasetDB.setDistributionIDs(distributionIDs);
					try {
						datasetDB.update();
					} catch (LODVaderMissingPropertiesException e) {
						e.printStackTrace();
					}
				}
			}

		}

	}

}
