/**
 * 
 */
package lodVader.parsers.ckanparser.utils;

import java.io.IOException;
import java.net.MalformedURLException;

import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.parsers.ckanparser.helpers.HTTPConnectionHelper;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 26, 2016
 */
public class CkanUtils {

	final static Logger logger = LoggerFactory.getLogger(CkanUtils.class);

	public static String unveil(String ckanCatalogAddress)
			throws JSONException, MalformedURLException, IOException {
		HTTPConnectionHelper connectionHelper = new HTTPConnectionHelper();

		logger.info("Unveiling CKAN version for: " + ckanCatalogAddress);

		logger.info("... making request: " + ckanCatalogAddress + "/api/3");
		if (connectionHelper.getJSONResponse(ckanCatalogAddress + "/api/3").get("version").toString().equals("3")) {
			logger.info("CKAN Version 3 compatible.");
			return "3";
		} else
			return null;
	}

}
