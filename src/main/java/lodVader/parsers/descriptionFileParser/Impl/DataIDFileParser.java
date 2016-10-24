/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionDB.DistributionStatus;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserInterface;
import lodVader.parsers.descriptionFileParser.helpers.DCATHelper;
import lodVader.parsers.descriptionFileParser.helpers.DataIDHelper;
import lodVader.utils.FormatsUtils;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class DataIDFileParser implements DescriptionFileParserInterface {

	final static Logger logger = LoggerFactory.getLogger(DataIDFileParser.class);

	ArrayList<DatasetDB> datasets = new ArrayList<>();

	ArrayList<DistributionDB> distributions = new ArrayList<>();

	DataIDHelper dataidHelper = new DataIDHelper();

	String repositoryAddress;

	/**
	 * Constructor for Class DataIDFileParser2
	 */
	public DataIDFileParser(String dcatFile) {
		this.repositoryAddress = dcatFile;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#
	 * getDistributions()
	 */
	@Override
	public List<DistributionDB> getDistributions() {
		return distributions;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getDatasets()
	 */
	@Override
	public List<DatasetDB> getDatasets() {
		return datasets;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {

		DCATHelper dcatHelper = new DCATHelper(repositoryAddress, "ttl");
		for (String catalog : dcatHelper.getListOfCatalogs()) {

			for (String dcatDataset : dcatHelper.getDatasetsFromCatalog(catalog)) {
				// repositoryAddress = dcatDataset;

				if (!dataidHelper.loadDataIDFile(dcatDataset, "ttl"))
					logger.error("We couldn't load the DataID file.");

				else {

					String dataset = dataidHelper.getPrimaryTopic();

					DatasetDB mainDataset = saveDataset(dataset, "");
					iterateDatasets(dataset, mainDataset);
					try {
						mainDataset.update();
					} catch (LODVaderMissingPropertiesException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}

		}

	}

	public DistributionDB saveDistribution(String distribution, DatasetDB dataset) {

		DistributionDB distributionDB = new DistributionDB(dataidHelper.getDownloadURL(distribution));
		distributionDB.setUri(distribution);
		distributionDB.setDefaultDatasets(new ArrayList<String>(Arrays.asList(dataset.getID())));
		distributionDB.setTopDataset(dataset.getID());
		distributionDB.setTopDatasetTitle(dataset.getTitle());
		distributionDB.setTitle(dataidHelper.getTitle(distribution));
		distributionDB.setLabel(dataidHelper.getLabel(distribution));
		if (distributionDB.getID() == null)
			distributionDB.setStatus(DistributionStatus.WAITING_TO_STREAM);
		distributionDB.setFormat(FormatsUtils.getEquivalentFormat(dataidHelper.getFormat(distribution)));
		distributionDB.setOriginalFormat(dataidHelper.getFormat(distribution));

		distributions.add(distributionDB);
		distributionDB.update(true, DistributionDB.DOWNLOAD_URL, distributionDB.getDownloadUrl());

		return distributionDB;
	}

	public void iterateDatasets(String dataset, DatasetDB parentDataset) {
		// get all subsets
		for (String subset : dataidHelper.getSubsets(dataset)) {
			iterateDatasets(subset, parentDataset);
		}

		DatasetDB datasetDB = saveDataset(dataset, parentDataset.getID());
		if (!dataset.equals(parentDataset.getID())) {
			parentDataset.addSubsetID(datasetDB.getID());
		}

		List<String> distributions = dataidHelper.getDistributions(dataset);
		for (String distribution : distributions) {
			DistributionDB distributionDB = saveDistribution(distribution, datasetDB);
			datasetDB.addDistributionID(distributionDB.getID());
			parentDataset.addDistributionID(distributionDB.getID());

		}

	}

	public DatasetDB saveDataset(String dataset, String parentDataset) {
		DatasetDB mainDataset = new DatasetDB(dataset);
		mainDataset.setTitle(dataidHelper.getTitle(dataset));
		mainDataset.setLabel(dataidHelper.getLabel(dataset));
		mainDataset.setIsVocabulary(false);
		mainDataset.setDescriptionFileParser(getParserName());
		mainDataset.addProvenance(repositoryAddress);
		mainDataset.update(true, DatasetDB.URI, dataset);

		datasets.add(mainDataset);

		return mainDataset;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getParserName(
	 * )
	 */
	@Override
	public String getParserName() {
		return "DATAID_PARSER";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#
	 * getRepositoryAddress()
	 */
	@Override
	public String getRepositoryAddress() {
		return repositoryAddress;
	}

}
