/**
 * 
 */
package org.aksw.idol.parsers.descriptionFileParser.lodstats.Impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.aksw.idol.ontology.RDFResourcesTags;
import org.aksw.idol.parsers.descriptionFileParser.Sparqles.Impl.SparqlesHTMLParser;
import org.aksw.idol.utils.ConnectionUtils;
import org.aksw.idol.utils.FormatsUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * @author Ciro Baron Neto
 * 
 * Dec 1, 2016
 */
public class LodStatsHTMLParser {

	final static Logger logger = LoggerFactory.getLogger(SparqlesHTMLParser.class);
	InputStream stream = null;

	/**
	 * Constructor for Class SparqlesRDFParser
	 */
	public LodStatsHTMLParser(InputStream stream) {
		this.stream = stream;
	}

	public List<String> parse(String address) {
		Document doc;
		List<String> r = new ArrayList<>();
		try {
			doc = Jsoup.connect(address).followRedirects(true).timeout(15000).get();

			// get all links
			Elements links = doc.select("td");
			for (Element element : links) {
				r.add(element.text());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		if (r.size() == 0)
			return null;
		return r;
	}
	
}
