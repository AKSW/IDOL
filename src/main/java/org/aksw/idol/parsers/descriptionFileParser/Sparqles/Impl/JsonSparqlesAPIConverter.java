/**
 * 
 */
package org.aksw.idol.parsers.descriptionFileParser.Sparqles.Impl;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

/**
 * Convert sparqles endpoint JSON to java classes
 * 
 * @author Ciro Baron Neto
 * 
 *         Nov 21, 2016
 */
public class JsonSparqlesAPIConverter {

	Gson gson = new Gson();

	String sparqlesEndpoint = null;

	/**
	 * Constructor for Class JsonSparqlesConverter
	 */
	public JsonSparqlesAPIConverter(String sparqlesEndpoint) {
		this.sparqlesEndpoint = sparqlesEndpoint;
	}

	/**
	 * Retrieve a class representing the endpoints available at the sparqles
	 * endpoint
	 * 
	 * @return
	 */
	public List<SparqlesAPIEndpoint> getList() {
		try {
			return new ArrayList<SparqlesAPIEndpoint>(
					Arrays.asList(gson.fromJson(
							new JsonReader(
									new InputStreamReader(new URL(sparqlesEndpoint).openConnection().getInputStream())),
							SparqlesAPIEndpoint[].class)));
		} catch (JsonIOException | JsonSyntaxException | IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
