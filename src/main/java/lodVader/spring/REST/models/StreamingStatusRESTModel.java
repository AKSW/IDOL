/**
 * 
 */
package lodVader.spring.REST.models;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;

/**
 * Model which represents the streaming process status.
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 30, 2016
 */
public class StreamingStatusRESTModel {

	private DecimalFormat formatter = new DecimalFormat("###,###,###,###,###,###");

	// the total number of triples
	private Double totalTriples = 0.0;
	
	// the total number of distributions with error
	private Integer totalDistributionsWithError =0;
	
	/**
	 * @return the totalDistributionsWithError
	 */
	public Integer getTotalDistributionsWithError() {
		return totalDistributionsWithError;
	}

	/**
	 * @return the totalBlankNodes
	 */
	public String getTotalTriples() {
		return formatter.format(totalTriples);
	}

	// the total number of blank nodes
	private Double totalBlankNodes = 0.0;

	/**
	 * @return the totalBlankNodes
	 */
	public String getTotalBlankNodes() {
		return formatter.format(totalBlankNodes);
	}

	// total number of distributions already processed (status=DONE)
	public Integer totalDistributionsProcessed = 0;

	/**
	 * @return the totalDistributionsProcessed
	 */
	public String getTotalDistributionsProcessed() {
		return formatter.format(totalDistributionsProcessed);
	}

	// total number of distribution to be processed (status=WAITING_TO_STREAM)
	public Integer totalDistributionsWaiting = 0;

	/**
	 * @return the totalDistributionsWaiting
	 */
	public String getTotalDistributionsWaiting() {
		return formatter.format(totalDistributionsWaiting);
	}

	/**
	 * Constructor for Class StreamingStatusRESTModel
	 */
	public StreamingStatusRESTModel() {
		// TODO Auto-generated constructor stub
	}

	// mapping of datasources and datasourcestatus
	public HashMap<String, DatasourceStatus> datasources = new HashMap<>();

	/**
	 * Method which iterates over all distributions and create the hashmap of
	 * status separated out by datasources
	 * 
	 * @return
	 */
	public HashMap<String, DatasourceStatus> checkStatus() {

		// fetch all distributions
		ArrayList<DBObject> objects = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
				new BasicDBObject());

		for (DBObject o : objects) {

			DistributionDB distributionDB = new DistributionDB(o);

			// for each datasource within the distribution, start or accumulate
			// the numbers
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
	 * 
	 * @param distributionDB
	 * @param datasource
	 */
	private void initializeDatasourceStatus(DistributionDB distributionDB, String datasource) {
		DatasourceStatus datasourceStatus = new DatasourceStatus();
		datasources.put(datasource, datasourceStatus);
		datasourceStatus.blankNodes = Double.valueOf(distributionDB.getBlankNodes());
		datasourceStatus.blankNodes = Double.valueOf(distributionDB.getNumberOfTriples());
		if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.DONE)) {
			datasourceStatus.distributionsProcessed = 1;
			datasourceStatus.distributionsWaiting = 0;
			datasourceStatus.distributionsWithError = 0;
		} else if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.WAITING_TO_STREAM)) {
			datasourceStatus.distributionsWaiting = 1;
			datasourceStatus.distributionsProcessed = 0;
			datasourceStatus.distributionsWithError = 0;			
		}else if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.ERROR)) {
			datasourceStatus.distributionsWaiting = 0;
			datasourceStatus.distributionsProcessed = 0;
			datasourceStatus.distributionsWithError = 1;			
		}
		datasourceStatus.setDatasource(datasource);
	}

	/**
	 * Update the datasourceStatus status within the map
	 * 
	 * @param distributionDB
	 * @param datasource
	 * @param datasourceStatus
	 */
	private void addToDataseurceStatus(DistributionDB distributionDB, String datasource,
			DatasourceStatus datasourceStatus) {
		datasourceStatus.blankNodes = datasourceStatus.blankNodes + distributionDB.getBlankNodes();
		datasourceStatus.triples = datasourceStatus.triples + distributionDB.getNumberOfTriples();

		if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.DONE)) {
			datasourceStatus.distributionsProcessed++;
			totalDistributionsProcessed++;
		} else if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.WAITING_TO_STREAM)) {
			datasourceStatus.distributionsWaiting++;
			totalDistributionsWaiting++;
		}else if (distributionDB.getStatus().equals(DistributionDB.DistributionStatus.ERROR)) {
			datasourceStatus.distributionsWithError++;
			totalDistributionsWithError++;
		}
	}

	/**
	 * Class which holds data for the status of each datasource
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Oct 30, 2016
	 */
	class DatasourceStatus {

		private String datasource;

		private Double blankNodes = 0.0;

		private Double triples = 0.0;

		public Integer distributionsProcessed = 0;

		public Integer distributionsWaiting = 0;

		public Integer distributionsWithError = 0;

		/**
		 * @return the distributionsWithError
		 */
		public Integer getDistributionsWithError() {
			return distributionsWithError;
		}
		
		/**
		 * @return the triples
		 */
		public String getTriples() {
			return formatter.format(triples);
		}

		/**
		 * @return the blankNodes
		 */
		public String getBlankNodes() {
			return formatter.format(blankNodes);
		}

		/**
		 * @return the distributionsProcessed
		 */
		public String getDistributionsProcessed() {
			return formatter.format(distributionsProcessed);
		}

		/**
		 * @return the distributionsWaiting
		 */
		public String getDistributionsWaiting() {
			return formatter.format(distributionsWaiting);
		}

		public Integer getDistributions() {
			return distributionsProcessed + distributionsWaiting;
		}

		// public Integer getNumberOfDatasets(){
		// String provenance = new
		// GeneralQueriesHelper().getObjects(DescriptionFileParserDB.COLLECTION_NAME,
		// DescriptionFileParserDB.PARSER_NAME,
		// datasource).iterator().next().get(DescriptionFileParserDB.REPOSITORY_ADDRESS).toString();
		// return new
		// GeneralQueriesHelper().getObjects(DatasetDB.COLLECTION_NAME,
		// DatasetDB.PROVENANCE, provenance).size();
		//
		// }

		/**
		 * @param datasource
		 *            Set the datasource value.
		 */
		public void setDatasource(String datasource) {
			this.datasource = datasource;
		}

	}

}
