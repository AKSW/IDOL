/**
 * 
 */
package org.aksw.idol.parsers.descriptionFileParser.Impl;

import java.util.List;

import org.aksw.idol.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.mongodb.collections.DatasetDB;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;
import org.aksw.idol.parsers.descriptionFileParser.helpers.DCATHelper;
import org.aksw.idol.parsers.descriptionFileParser.helpers.DataIDHelper;
import org.aksw.idol.utils.FormatsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class DataIDParser extends MetadataParser {

	final static Logger logger = LoggerFactory.getLogger(DataIDParser.class);

	DataIDHelper dataidHelper = new DataIDHelper();

	String repositoryAddress;

	/**
	 * Constructor for Class DataIDFileParser2
	 */
	public DataIDParser(String dcatFile) {
		super("DATAID_PARSER");
		this.repositoryAddress = dcatFile;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see idol.parsers.interfaces.DescriptionFileParserInterface#parse()
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
		
		String downloadURL = (distribution);
		String uri = distribution;
		String title = dataidHelper.getTitle(distribution);
		String topDataset = dataset.getID();
		String topDatasetTitle = dataset.getTitle();
		String format = dataidHelper.getFormat(distribution);
		if(format.equals(""))
			format=FormatsUtils.getEquivalentFormat(downloadURL);
		
		return addDistribution(uri, false, title, format, downloadURL, topDataset, topDatasetTitle, getParserName(), repositoryAddress, null, null);
	}

	public void iterateDatasets(String dataset, DatasetDB parentDataset) {
		// get all subsets
//		for (String subset : dataidHelper.getSubsets(dataset)) {
//			iterateDatasets(subset, parentDataset);
//		}

		DatasetDB datasetDB = saveDataset(dataset, parentDataset.getID());
//		if (!dataset.equals(parentDataset.getID())) {
//			parentDataset.addSubsetID(datasetDB.getID());
//		}

		List<String> distributions = dataidHelper.getDistributions(dataset);
		System.out.println(distributions.size());
		for (String distribution : distributions) {
			DistributionDB distributionDB = saveDistribution(distribution, datasetDB);
			System.out.println(distributionDB.getDownloadUrl());
			datasetDB.addDistributionID(distributionDB.getID());
			parentDataset.addDistributionID(distributionDB.getID());
		}

	}

	public DatasetDB saveDataset(String dataset, String parentDataset) {
		
		return addDataset(dataset, false, dataidHelper.getTitle(dataset), dataidHelper.getLabel(dataset), getParserName());


	}

}
