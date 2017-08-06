package org.aksw.idol.properties.parsers;

import java.net.URL;

public class SparqlesParserProperties {
	
	public boolean stream;
	
	public URL url;

	public boolean isStream() {
		return stream;
	}

	public void setStream(boolean stream) {
		this.stream = stream;
	}

	public URL getUrl() {
		return url;
	}

	public void setUrl(URL url) {
		this.url = url;
	}

}
