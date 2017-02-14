package org.aksw.idol.utils;

import java.net.MalformedURLException;
import java.net.URLEncoder;

public class URLUtils {
	
	public void validateURL(String url) throws MalformedURLException {
		if (!url.startsWith("http"))
			throw new MalformedURLException("Bad URL: " + url);
	}

} 
