/**
 * 
 */
package lodVader.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;

/**
 * @author Ciro Baron Neto
 * 
 * Nov 23, 2016
 */
public class ConnectionUtils {

	
	public final static String RDF_HEADER = "application/rdf+xml";
	public final static String TTL_HEADER = "text/turtle";
	public final static String JSON_HEADER = "application/json";

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
	
	
	/**
	 * Open a connection and return the response string
	 * @param URL the URL to be opened
	 * @param accept the accept header
	 * @return the response
	 * @throws IOException
	 */
	public String getResponseString(String URL, String accept) throws IOException {
		HttpURLConnection httpConn;
		httpConn = (HttpURLConnection) new URL(URL).openConnection();
		httpConn.setReadTimeout(14000);
		httpConn.setConnectTimeout(14000);
		if (accept != null)
			httpConn.setRequestProperty("Accept", accept);
		else
			httpConn.setRequestProperty("Accept", "application/rdf+xml");
		
		
		InputStream in =httpConn.getInputStream();

		StringBuilder builder = new StringBuilder();
		 try {
		   builder.append(IOUtils.toString( in ));
		 } finally {
		   IOUtils.closeQuietly(in);
		 }
		return builder.toString();
	}
	
	
}
