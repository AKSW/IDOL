/**
 * 
 */
package lodVader.parsers.descriptionFileParser.helpers;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.ckanparser.CkanDatasetList;
import lodVader.parsers.ckanparser.CkanParser;
import lodVader.parsers.ckanparser.models.CkanDataset;
import lodVader.parsers.ckanparser.models.CkanResource;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class CKANHelper {
	CkanParser client;

	List<DistributionDB> distributions = new ArrayList<>();

	List<DatasetDB> datasets = new ArrayList<>();

	/**
	 * Constructor for Class CKANHelper
	 */
	public CKANHelper(String URI) {
		client = new CkanParser(URI);
	}

	public CkanDatasetList getDatasetList() {
		return new CkanDatasetList(client);
//		return client.getDatasetList();
	}

	public void saveInstances(List<String> ds) {

		ExecutorService executor = Executors.newFixedThreadPool(2);

		for (String s : ds) {
			WorkerThread worker = new WorkerThread(s);
			executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
	}

	public DatasetDB saveDataset(CkanDataset dataset) {
		DatasetDB datasetDB = new DatasetDB(dataset.getId());
		datasetDB.setIsVocabulary(false);
		datasetDB.setTitle(dataset.getTitle());
		datasetDB.setLabel(dataset.getTitle());
		datasetDB.setUri(dataset.getId()); 
		try {
			datasetDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return datasetDB;
	}

	public DistributionDB saveDistribution(CkanResource resource) {

		DistributionDB distributionDB = new DistributionDB();
		try {
			distributionDB.setTitle(resource.getId());
			distributionDB.setDownloadUrl(resource.getUrl());
			distributionDB.setFormat(resource.getFormat());
			distributionDB.update();
		} catch (LODVaderMissingPropertiesException
				| MalformedURLException e) {
			// TODO Auto-generated catch block 
			e.printStackTrace();
		}

		return distributionDB;

	}

	public class WorkerThread implements Runnable {

		String dataset;

		/**
		 * Constructor for Class CKANHelper.WorkerThread
		 */
		public WorkerThread(String ckanDataset) {
			this.dataset = ckanDataset;
		}

		@Override
		public void run() {
			CkanDataset d = client.fetchDataset(dataset);
			DatasetDB datasetDB = saveDataset(d);

			for (CkanResource r : d.getResources()) {
				DistributionDB distributionDB = saveDistribution(r);
				datasetDB.addDistributionID(distributionDB.getID());
			}

			try {
				datasetDB.update();
			} catch (LODVaderMissingPropertiesException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
