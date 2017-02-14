/**
 * 
 */
package org.aksw.idol.parsers.ckanparser.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.jena.atlas.json.io.parser.JSONParser;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author Ciro Baron Neto
 * 
 *         Aug 25, 2016
 */
public class HTTPConnectionHelper {

	private HttpURLConnection connection;

	private Integer responseCode = null;

	JSONParser parser = new JSONParser();

	/**
	 * Opens a new connection
	 * 
	 * @param link
	 *            the URL for the connection
	 * @param untrustCertificate
	 *            trust or not the SSL certificate (for https connections)
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public void openConnection(String link, boolean untrustCertificate) throws MalformedURLException, IOException {
		// System.setProperty("javax.net.debug", "all");
		// System.setProperty("https.protocols", "SSLv3");
		// System.setProperty("https.protocols", "TLSv1");

		connection = (HttpURLConnection) new URL(link).openConnection();
		setRequestProperties();
		try {
			responseCode = connection.getResponseCode();
		} catch (SSLProtocolException | SSLHandshakeException e) {
			untrustCertificate();
			connection = (HttpURLConnection) new URL(link).openConnection();
			setRequestProperties();
			responseCode = connection.getResponseCode();
		}
	}

	/**
	 * Opens a new connection and set cookies
	 * 
	 * @param link
	 * @param cookies
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public void openConnection(String link, String cookies) throws MalformedURLException, IOException {
		connection = (HttpURLConnection) new URL(link).openConnection();
		setRequestProperties();
		connection.setRequestProperty("Cookie", cookies);
		responseCode = connection.getResponseCode();
	}

	private void setRequestProperties() {
		connection.setRequestProperty("Accept", "application/rdf+xml");
		connection.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
		connection.addRequestProperty("User-Agent", "Mozilla");
		connection.addRequestProperty("Referer", "google.com");
	}

	/**
	 * Return HTTP response code
	 * 
	 * @return
	 */
	public Integer getResponseCode() {
		if (responseCode != null)
			return responseCode;
		else
			throw new NullPointerException();
	}

	public HttpURLConnection getHttpConnection() {
		return connection;
	}

	public JSONObject getJSONResponse(String url) throws MalformedURLException, IOException {
		JSONObject o;
		try {
			openConnection((url), true);
			o = readJsonFromUrl();
		} 
		finally {
			getHttpConnection().getInputStream().close();
		}
		return o;
	}

	private String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public JSONObject readJsonFromUrl() throws IOException, JSONException {
		BufferedReader rd = new BufferedReader(
				new InputStreamReader(getHttpConnection().getInputStream(), Charset.forName("UTF-8")));
		String jsonText = readAll(rd);
		JSONObject json = null;
			json = new JSONObject(jsonText);
		return json;
	}

	private void untrustCertificate() {
		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(X509Certificate[] certs, String authType) {
			}

			public void checkServerTrusted(X509Certificate[] certs, String authType) {
			}
		} };

		// Install the all-trusting trust manager
		SSLContext sc;
		try {
			// sc = SSLContext.getInstance("SSL");
			// sc = SSLContext.getInstance("TLSv1.2");
			sc = SSLContext.getInstance("TLSv1");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

			// Create all-trusting host name verifier
			HostnameVerifier allHostsValid = new HostnameVerifier() {

				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};
			// Install the all-trusting host verifier
			HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
		} catch (NoSuchAlgorithmException | KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
