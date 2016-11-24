/**
 * 
 */
package lodVader.parsers.descriptionFileParser;

import java.util.HashMap;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.services.mongodb.DatasetServices;
import lodVader.services.mongodb.DistributionServices;

/**
 * @author Ciro Baron Neto
 * 
 * Nov 19, 2016
 */
public abstract class MetadataParser {
	
	String parserName;
	
	HashMap<String, DistributionDB> distributions = new HashMap<>();
	
	HashMap<String, DatasetDB> datasets = new HashMap<>();
	
	DatasetServices datasetServices = new DatasetServices();
	
	DistributionServices distributionServices = new DistributionServices();
	
	
	/**
	 * Constructor for Class MetadataParser 
	 */
	public MetadataParser(String parserName) {
		this.parserName = parserName;
	}
	
	
	/**
	 * Add or update a distribution on the list
	 * @param dataset the dataset to be added
	 */
	public void addDataset(DatasetDB dataset){
			datasets.put(dataset.getUri(), dataset);
	}
	
	/**
	 * @return the parserName
	 */
	public String getParserName() {
		return parserName;
	}
	
	
	/**
	 * Add a new dataset in the list. The method will save it and return an respective ID
	 * @param uri the dataset uri
	 * @param isVocab represents if the dataset is a vocabulary or a ontology
	 * @param title the dataset title
	 * @param labelthe dataset label
	 * @param provenance whe the dataset came from
	 * @return the dataset 
	 */
	public DatasetDB addDataset(String uri, boolean isVocab, String title, String label, String provenance){
		DatasetDB d = datasetServices.saveDataset(uri, isVocab, title, label, provenance);	
		addDataset(d);
		return d;
	}
	
	/**
	 * Add a new distribution to the list
	 * @param uri the distribution URI
	 * @param isVocab represents if the dataset is a vocabulary or a ontology
	 * @param title the distriution title
	 * @param format the distribution format
	 * @param downloadURL the distribution downloadURL
	 * @param topDataset the top dataset
	 * @param topDatasetTitle the top dataset title
	 * @param datasource the datasource which holds the distribution
	 * @param repository the repository which holds the distribution
	 * @return the distribution 
	 */
	public DistributionDB addDistribution(String uri, boolean isVocab, String title, String format, String downloadURL,
			String topDataset, String topDatasetTitle, String datasource, String repository, String sparqlGraph){
		DistributionDB d = distributionServices.saveDistribution(uri, isVocab, title, format, downloadURL,
				topDataset, topDatasetTitle, datasource, repository, sparqlGraph);
		addDistribution(d);
		return d;
	}	
	
	
	
	/**
	 * Add or update a distribution in the list
	 * @param distribution
	 */
	public void addDistribution(DistributionDB distribution){
		distributions.put(distribution.getDownloadUrl(), distribution);
	}
	
	/**
	 * @return the distributions
	 */
	public HashMap<String, DistributionDB> getDistributions() {
		return distributions;
	}
	
	/**
	 * @return the datasets
	 */
	public HashMap<String, DatasetDB> getDatasets() {
		return datasets;
	}
	
	
	/**
	 * Get a single distribution by the downloadURL
	 * @param the ditribution's downloadURL
	 * @return the distributions
	 */
	public DistributionDB getDistribution(String downloadURL){
		return distributions.get(downloadURL);
	}
	
	/**
	 * Get a single dataset by the URI
	 * @param url
	 * @return
	 */
	public DatasetDB getDataset(String URI){
		return datasets.get(URI);
	}
	

	public abstract void parse();

}
