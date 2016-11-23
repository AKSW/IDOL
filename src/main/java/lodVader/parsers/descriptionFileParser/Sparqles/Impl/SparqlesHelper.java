/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Sparqles.Impl;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 21, 2016
 */
public class SparqlesHelper {

	final static Logger logger = LoggerFactory.getLogger(SparqlesHelper.class);

	/**
	 * Make content negotiation and use different parsers in order to get a list
	 * of distribution
	 * 
	 * @throws IOException
	 */
	public Collection<String> getDistributions(String endpointAddress) throws IOException {

		String downloadURL = endpointAddress + "?query="
				+ URLEncoder.encode("select distinct ?g where { graph ?g {?s ?p ?o} }");

		logger.info("Loading distribution within endpoint: " + endpointAddress);
		List<String> distributions = null;
		
		SparqlesConnection conn = new SparqlesConnection();

		SparqlesDistributionParser parser = new SparqlesRDFParser(conn.getStream(downloadURL, SparqlesConnection.TTL_HEADER));
		/**
		 * Try to parse RDF
		 */
		logger.info("Trying to parse RDF.");
		distributions = parser.parse(downloadURL);
		if (distributions != null) {
			logger.info("Success. The endpoint: " + endpointAddress + " responded with RDF data.");
			return distributions;
		}

		/**
		 * If fails, try to parse HTML
		 */
		logger.info("Failed! Trying to parse HTML with parser 1.");
		parser = new SparqlesHTMLParser();
		distributions = parser.parse(downloadURL);

		/**
		 * If fails again, try with a different parser
		 */
		if (distributions != null) {
			logger.info("Success. The endpoint: " + endpointAddress + " responded with valid HTML data.");
			return distributions;
		}

		logger.info("Failed! Trying to parse HTML with XML sparql parser.");
		parser = new SparqlesXMLParser();
		distributions = parser.parse(downloadURL);

		if (distributions != null) {
			logger.info("Success. The endpoint: " + endpointAddress + " responded with valid sparql XML data.");
			return distributions;
		}
		
		logger.info("Failed! We've got an invalid response.");
		return null;
	}

}
