package org.aksw.idol.utils;

import org.apache.commons.lang.StringUtils;

public class NSUtils {

	public String getNS0(String url) {
		if (url.length() > 1024)
			url = url.substring(0, 1024);

		String[] split = url.split("/");
		if (split.length > 3)
			url = split[0] + "//" + split[2] + "/";
		else if (!url.endsWith("/"))
			url = url + "/";
		if (url.startsWith("htt"))
			return url;
		else
			return "";
	}

	public String getNS1(String url) {
		if (url.startsWith("htt"))
			if (url.length() > 1024)
				url = url.substring(0, 1024);

		String[] split = url.split("/");
		if (split.length > 4)
			url = split[0] + "//" + split[2] + "/" + split[3] + "/";
		else
			return null;
		if (url.startsWith("htt"))
			return url;
		else
			return "";
	}

	public String getNSFromString(String url) {

		if (url.length() > 1024)
			url = url.substring(0, 1024);

		String[] split = url.split("/");
		int total = split.length;

		if (total <= 7) {
			int index = url.lastIndexOf("#");
			if (index == -1)
				index = url.lastIndexOf("/");

			return url.substring(0, index + 1);
		} else {
			int index = StringUtils.ordinalIndexOf(url, "/", 5);
			return url.substring(0, index + 1);
		}
	}



	public String getNSFromString(String url, int nsLevel) {
		if (url.length() > 1024)
			url = url.substring(0, 1024);

		nsLevel = nsLevel + 3;
		String[] split = url.split("/");
		int total = split.length;

		if (total <= nsLevel) {
			int index = url.lastIndexOf("#");
			if (index == -1)
				index = url.lastIndexOf("/");

			return url.substring(0, index + 1);
		} else {
			int index = StringUtils.ordinalIndexOf(url, "/", nsLevel);
			return url.substring(0, index + 1);
		}
	}

}
