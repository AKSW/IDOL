/**
 * 
 */
package lodVader.parsers.ckanparser;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.executors.LodVaderExecutor;
import lodVader.parsers.ckanparser.helpers.HTTPConnectionHelper;
import lodVader.parsers.ckanparser.models.CkanCatalog;
import lodVader.parsers.ckanparser.models.CkanDataset;
import lodVader.parsers.ckanparser.models.CkanDatasetAdapterV3;
import lodVader.parsers.ckanparser.requestoperations.CkanParserRequestOperations;
import lodVader.parsers.ckanparser.requestoperations.impl.V3;
import lodVader.parsers.ckanparser.utils.CkanUtils;

/**
 * CKAN parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 26, 2016
 */
public class CkanParser implements CkanParserInterface {

	final static Logger logger = LoggerFactory.getLogger(CkanParser.class);

	// For each CKAN version the request operations are different.
	CkanParserRequestOperations operations;

	// Class which holds all CKAN catalog information
	CkanCatalog ckanCatalog = new CkanCatalog();

	HTTPConnectionHelper connectionHelper = new HTTPConnectionHelper();

	List<String> datasetIds;

	CkanDatasetList datasetList = new CkanDatasetList(this);

	/**
	 * Constructor for Class CkanParser
	 */
	public CkanParser(String ckanCatalogAddress) {
		this.ckanCatalog.setCatalogAddress(ckanCatalogAddress);
	}

	/**
	 * @return the ckanCatalog
	 */
	public CkanCatalog getCkanCatalog() {
		getOperations();
		return ckanCatalog;
	}

	private List<String> getPackageListFromCatalog() {
		try {
			datasetIds = new ArrayList<>();
			connectionHelper.getJSONResponse(getOperations().makeDatasetListRequest(ckanCatalog.getCatalogAddress()))
					.getJSONArray("result").forEach((id) -> {
						datasetIds.add(id.toString());
					});
		} catch (JSONException e) {
			e.printStackTrace();
			logger.error("It seems that the CKAN catalog: " + ckanCatalog.getCatalogAddress()
					+ " does not have the 'result' key. ");
			logger.error("Let' s try something else... Maybe get the 'results' array within the 'result' object.  ");

			try {

				List<Future<List<String>>> datasetsFuture = new ArrayList<>();
				
				// defining an executor (lambda)
				LodVaderExecutor executor = ((url) -> {
					
					// wait some random time in order to not be denied by the server
					try {
						Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 5000 + 1));
					} catch (InterruptedException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					logger.info("request done " + url);
					
					List<String> datasets = new ArrayList<>();
					try {
						HTTPConnectionHelper connectionHelperThread = new HTTPConnectionHelper();
						connectionHelperThread
								.getJSONResponse(url)
								.getJSONObject("result").getJSONArray("results").forEach((pack) -> {
									byte ptext[] = pack.toString().getBytes(Charset.forName("ISO-8859-1"));
									String value = new String(ptext, Charset.forName("UTF-8"));
									datasets.add(new JSONObject(value).get("id").toString());
								});
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					return datasets;
				});
				

				logger.info("Get the counter of how many packages we have.");
				int size = connectionHelper
						.getJSONResponse(getOperations().makeDatasetListRequest(ckanCatalog.getCatalogAddress()))
						.getJSONObject("result").getInt("count");
				logger.info("We have found: " + size + " packages.");
				
				int counter = 0;

				int step = 200; 
				while (counter < size) {
					logger.info("Making request with pagination: " + getOperations()
							.makeDatasetListRequestPagination(ckanCatalog.getCatalogAddress(), step, counter));
					datasetsFuture.add(executor.execute(getOperations()
							.makeDatasetListRequestPagination(ckanCatalog.getCatalogAddress(), step, counter)));
					counter = counter + step;
				}

				
				executor.shutdown();
				for(Future<List<String>> future : datasetsFuture ){
					try {
						datasetIds.addAll(future.get());
					} catch (InterruptedException | ExecutionException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
				logger.info("We have fond " + datasetIds.size() + " datasets.");

			} catch (JSONException | IOException e1) {
				e1.printStackTrace();
				logger.error("Failed. We have to skip the catalog: " + ckanCatalog.getCatalogAddress());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return datasetIds;
	}

	/**
	 * @return the operations
	 */
	public CkanParserRequestOperations getOperations() {
		if (operations == null) {
			try {
				if (operations == null) {
					if (CkanUtils.unveil(ckanCatalog.getCatalogAddress()).equals("3")) {
						operations = new V3();
						ckanCatalog.setVersion(CkanCatalog.VERSION.V3);
					}
				}
			} catch (JSONException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return operations;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.ckanparser.CkanParserInterface#fetchDataset(java.lang.
	 * String)
	 */
	@Override
	public CkanDataset fetchDataset(String datasetID) {
		CkanDataset ckanDataset = new CkanDataset();

		try {
			ckanDataset = new CkanDatasetAdapterV3(
					new JSONObject(connectionHelper
							.getJSONResponse(
									getOperations().makeDatasetRequest(ckanCatalog.getCatalogAddress(), datasetID))
							.get("result").toString()));
		} catch (JSONException | IOException e) {
//			System.out.println(connectionHelper.getResponseCode());
			e.printStackTrace();
		}

		logger.info("Loaded dataset: " + ckanDataset.getId());
		return ckanDataset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.ckanparser.CkanParserInterface#fetchDatasetIds()
	 */
	@Override
	public List<String> fetchDatasetIds() {
		if (datasetIds == null)
			getPackageListFromCatalog();
		return datasetIds;
	}

	/**
	 * @return the datasetList
	 */
	public CkanDatasetList getDatasetList() {
		getOperations();
		return datasetList;
	}

}
