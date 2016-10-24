/**
 * 
 */
package lodVader.parsers.descriptionFileParser;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.loader.LODVaderConfigurator;
import lodVader.mongodb.collections.DescriptionFileParserDB;
import lodVader.services.mongodb.dataset.DatasetServices;
import lodVader.services.mongodb.distribution.DistributionServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Sep 11, 2016
 */
public class DescriptionFileParserLoader {

	final static Logger logger = LoggerFactory.getLogger(DescriptionFileParserLoader.class);

	private DescriptionFileParserInterface parser = null;
	
	DescriptionFileParserDB parserDB;


	/**
	 * Save all distributions and datasets from a metadata file parser as
	 * MongoDB documents
	 * 
	 * @return
	 */
	public boolean parse() {

		// check whether the user already loaded a parser
		if (parser == null)
			return false;
		DatasetServices datasetServices = new DatasetServices();
		DistributionServices distributionServices = new DistributionServices();

		logger.info("Parsing " + parser.getParserName() + " for " + parser.getRepositoryAddress() + " repository.");

		parser.parse();
		datasetServices.saveAllDatasets(parser.getDatasets());
		logger.info("Parser saved " + parser.getDatasets().size() + " datasets!");

		distributionServices.saveAllDistributions(parser.getDistributions(), parser.getRepositoryAddress(), parser.getParserName());
		logger.info("Parser saved " + parser.getDistributions().size() + " distributions!");
		
		parserDB.setLastTimeUsed(String.valueOf(new Date().getTime()));
		try {
			parserDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return true;

	}

	/**
	 * Load a parser into this loader
	 * 
	 * @param parser
	 * @return true if the parser has already been loaded into database
	 */
	public boolean load(DescriptionFileParserInterface parser) {
		this.parser = parser;
		this.parserDB = new DescriptionFileParserDB(parser);
		if(this.parserDB.getLastTimeUsed() == null)
			return false;
		return true;
		
	}

}
