/**
 * 
 */
package org.aksw.idol.parsers.descriptionFileParser.Sparqles.Impl;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.aksw.idol.utils.ConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 22, 2016
 */
public class SparqlesXMLParser implements SparqlesDistributionParser {
	final static Logger logger = LoggerFactory.getLogger(SparqlesXMLParser.class);

	ConnectionUtils conn = new ConnectionUtils();
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.descriptionFileParser.Sparqles.Impl.
	 * SparqlesDistributionParser#parse(java.lang.String)
	 */
	@Override
	public List<String> parse(String address) {
		
		List<String> r = new ArrayList<>();

		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		dbFactory.setValidating(false);
		dbFactory.setAttribute("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		DocumentBuilder dBuilder;

		try {
			dBuilder = dbFactory.newDocumentBuilder();
			org.w3c.dom.Document doc = dBuilder.parse(conn.getStream(address, null));
			doc.getDocumentElement().normalize();

			NodeList nList = doc.getElementsByTagName("uri");

			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				r.add(nNode.getTextContent());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		if (r.size() == 0)
			return null;
		return r;
	}

}
