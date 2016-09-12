/**
 * 
 */
package lodVader.parsers.descriptionFileParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.loader.StartLODVader;
import lodVader.parsers.interfaces.DescriptionFileParserInterface;
import services.mongodb.dataset.DatasetServices;
import services.mongodb.distribution.DistributionServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 11, 2016
 */
public class DescriptionFileParserLoader {
	
	final static Logger logger = LoggerFactory.getLogger(DescriptionFileParserLoader.class);

	/**
	 * Save all distributions and datasets from a metadata file parser as MongoDB documents
	 * @param fileParser
	 */
	public static void load(DescriptionFileParserInterface fileParser) {

		DatasetServices datasetServices = new DatasetServices();
		DistributionServices distributionServices = new DistributionServices();

		fileParser.parse();
		datasetServices.saveAllDatasets(fileParser.getDatasets());
		logger.info("Parser saved "+fileParser.getDatasets().size()+" datasets!");
		
		distributionServices.saveAllDistributions(fileParser.getDistributions());
		logger.info("Parser saved "+fileParser.getDistributions().size()+" distributions!");
		

	}

}
