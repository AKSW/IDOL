/**
 * 
 */
package idol.parsers.descriptionFileParser.Impl;

/**
 * CKAN parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class RE3RepositoriesParser extends CKANParserIMPL {

	/**
	 * Constructor for Class CKANRepositoriesParser 
	 * @param parserName
	 * @param repositoryAddress
	 * @param numberOfConcurrentRequests
	 */
	public RE3RepositoriesParser(String repositoryAddress, int numberOfConcurrentRequests) {
		super("RE3_REPOSITORIES", repositoryAddress, numberOfConcurrentRequests);
	}



}
