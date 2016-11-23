/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Sparqles.Impl;

import java.util.Collection;

/**
 * Class representing an Sparqles endpoint (provided by the API
 * http://sparqles.ai.wu.ac.at/api/endpoint/list)
 * 
 * @author Ciro Baron Neto
 * 
 *         Nov 21, 2016
 */
public class SparqlesAPIEndpoint {

	String uri;

	Collection<Dataset> datasets;	
	
	class Dataset {
		String uri;
		String label;
	}
	
}
