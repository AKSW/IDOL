/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Sparqles.Impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 22, 2016
 */
public class SparqlesConnection {

	final static String RDF_HEADER = "application/rdf+xml";
	final static String TTL_HEADER = "text/turtle";
	final static String JSON_HEADER = "application/json";

	/**
	 * Open a connection and return the stream
	 * @param downloadURL
	 * @param accept
	 * @return
	 * @throws IOException
	 */
	public InputStream getStream(String downloadURL, String accept) throws IOException {
		HttpURLConnection httpConn;
		httpConn = (HttpURLConnection) new URL(downloadURL).openConnection();
		httpConn.setReadTimeout(14000);
		httpConn.setConnectTimeout(14000);
		if (accept != null)
			httpConn.setRequestProperty("Accept", accept);
		else
			httpConn.setRequestProperty("Accept", "application/rdf+xml");

		return new BufferedInputStream(httpConn.getInputStream());
	}

}
