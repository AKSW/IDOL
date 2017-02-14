/**
 * 
 */
package org.aksw.idol.parsers.descriptionFileParser.Impl;

/**
 * CKAN parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class CKANRepositoriesParser extends CKANParserIMPL {

	/**
	 * Constructor for Class CKANRepositoriesParser 
	 * @param parserName
	 * @param repositoryAddress
	 * @param numberOfConcurrentRequests
	 */
	public CKANRepositoriesParser(String repositoryAddress, int numberOfConcurrentRequests) {
		super("CKAN_REPOSITORIES", repositoryAddress, numberOfConcurrentRequests);
	}
	
	/**
	 * Constructor for Class CKANRepositoriesParser 
	 */
	public CKANRepositoriesParser() {
		super("CKAN_REPOSITORIES", null, 0);		
	}



}
