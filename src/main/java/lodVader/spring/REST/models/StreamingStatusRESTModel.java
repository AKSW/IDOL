/**
 * 
 */
package lodVader.spring.REST.models;

import java.util.ArrayList;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;

/**
 * Model which represents the streaming process status.
 * @author Ciro Baron Neto
 * 
 *         Oct 30, 2016
 */
public class StreamingStatusRESTModel {

	// the total number of triples
	public Double totalTriples = 0.0;

	// the total number of blank nodes
	public Double totalBlankNodes = 0.0;

	// total number of distributions already processed (status=DONE)
	public Integer totalDistributionsProcessed = 0;

	// total number of distribution to be processed (status=WAITING_TO_STREAM)
	public Integer totalDistributionsWaiting = 0;

	/**
	 * Constructor for Class StreamingStatusRESTModel
	 */
	public StreamingStatusRESTModel() {
		// TODO Auto-generated constructor stub
	}

	// mapping of datasources and datasourcestatus
	public HashMap<String, DatasourceStatus> datasources = new HashMap<>();

	/**
	 * Method which iterates over all distributions and create the hashmap of status separated out by datasources
	 * @return
	 */
	public HashMap<String, DatasourceStatus> checkStatus() {

		//fetch all distributions
		ArrayList<DBObject> objects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		for (DBObject o : objects) {

			DistributionDB distributionDB = new DistributionDB(o);

			// for each datasource within the distribution, start or accumulate the numbers
			for (String datasource : distributionDB.getDatasources()) {

				if (datasources.get(datasource) == null) {
					initializeDatasourceStatus(distributionDB, datasource);
				} else {
					addToDataseurceStatus(distributionDB, datasource, datasources.get(datasource));
				}
				
				// for each distribution, increment the counters
				totalBlankNodes = totalBlankNodes + distributionDB.getBlankNodes();
				totalTriples = totalTriples + distributionDB.getNumberOfTriples();
			}

		}

		return datasources;
	}

	/**
	 * Initialize a datasourceStatus class within the map
	 * @param distributionDB
	 * @param datasource
	 */
	private void initializeDatasourceStatus(DistributionDB distributionDB, String datasource) {
		DatasourceStatus datasourceStatus = new DatasourceStatus();
		datasources.put(datasource, datasourceStatus);
		datasourceStatus.blankNodes = distributionDB.getBlankNodes();
		if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.DONE)) {
			datasourceStatus.distributionsProcessed = 1;
			datasourceStatus.distributionsWaiting = 0;
		} else if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.WAITING_TO_STREAM)) {
			datasourceStatus.distributionsWaiting = 1;
			datasourceStatus.distributionsProcessed = 0;
		}
	}
	
	/**
	 * Update the datasourceStatus status within the map
	 * @param distributionDB
	 * @param datasource
	 * @param datasourceStatus
	 */
	private void addToDataseurceStatus(DistributionDB distributionDB, String datasource,
			DatasourceStatus datasourceStatus) {
		datasourceStatus.blankNodes = distributionDB.getBlankNodes();
		if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.DONE)){
			datasourceStatus.distributionsProcessed++;
			totalDistributionsProcessed ++;
		}
		else if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.WAITING_TO_STREAM)){
			datasourceStatus.distributionsWaiting++;
			totalDistributionsWaiting++;
		}
	}

	/**
	 * Class which holds data for the status of each datasource
	 * @author Ciro Baron Neto
	 * 
	 * Oct 30, 2016
	 */
	class DatasourceStatus {

		public Integer blankNodes = 0;

		public Integer distributionsProcessed = 0;

		public Integer distributionsWaiting = 0;

	}

}
