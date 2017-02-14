package org.aksw.idol.core.parsers.descriptionFileParser;

import java.util.List;

import org.aksw.idol.core.mongodb.collections.DatasetDB;
import org.aksw.idol.core.mongodb.collections.DistributionDB;


public interface MetadataParserI {
	
	/**
	 * Return a list of distributions found after the crawling process
	 * @return a list of DistributionDB instances
	 */
	public List<DistributionDB> getDistributions();
	
	/**
	 * Return a list of datasets found after the crawling process
	 * @return a list of DatasetDB instances	 
	 */
	public List<DatasetDB> getDatasets();
	
	/**
	 * Parse the description file/URL
	 */
	public void parse();
	
	/**
	 * Return the parser name
	 * @return the parser name
	 */
	public String getParserName();
	
	/**
	 * Get repository address
	 * @return the repository address
	 */
	public String getRepositoryAddress();

}
