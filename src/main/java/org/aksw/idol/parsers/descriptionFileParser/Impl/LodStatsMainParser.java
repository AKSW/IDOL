/**
 * 
 */
package org.aksw.idol.parsers.descriptionFileParser.Impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.aksw.idol.mongodb.collections.DatasetDB;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.ontology.RDFResourcesTags;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;
import org.aksw.idol.utils.ConnectionUtils;
import org.aksw.idol.utils.FormatsUtils;
import org.aksw.idol.utils.SPARQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Linghub parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class LodStatsMainParser extends MetadataParser {

	final static Logger logger = LoggerFactory.getLogger(LodStatsMainParser.class);

	String repositoryAddress;

	int threads = 20;

	/**
	 * Constructor for Class LodCloudParser
	 */
	public LodStatsMainParser(String URL) {
		super("LODSTATS_PARSER");
		this.repositoryAddress = URL;
	}

	/**
	 * Save a linghub dataset
	 * 
	 * @param the
	 *            CkanDataset
	 * @return the DatasetDB instance
	 */
	public DatasetDB saveDataset(String url, String title) {

		return addDataset(url, false, title, title, getParserName());

	}

	/**
	 * Save a CkanResource instance the main collection (a distribution)
	 * 
	 * @param the
	 *            CkanResource
	 * @return the DistributionDB instance
	 */
	public DistributionDB saveDistribution(String url, String title, String format, DatasetDB datasetDB,
			String sparqlGraph, String sparqlEndpoint) {

		return addDistribution(url, false, title, format, url, datasetDB.getID(), datasetDB.getTitle(), getParserName(),
				repositoryAddress, sparqlGraph, sparqlEndpoint);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {
		ConnectionUtils connUtils = new ConnectionUtils();
		SPARQLUtils sparqlUtils = new SPARQLUtils();

		// CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }
		// String query = "select ?o where {?s
		// <http://www.w3.org/ns/dcat#downloadURL> ?o .} ";
		String query = "CONSTRUCT { ?s <http://www.w3.org/ns/dcat#downloadURL> ?o } WHERE { ?s <http://www.w3.org/ns/dcat#downloadURL> ?o  }";

		String fullRequest = "http://stats.lod2.eu/sparql/?query=" + query;

		Model m = ModelFactory.createDefaultModel();
		FormatsUtils formatutils = new FormatsUtils();

		try {
			m.read(connUtils.getStream(sparqlUtils.encodeSparqlQuery(fullRequest), ConnectionUtils.TTL_HEADER), null,
					formatutils.getJenaFormat("ttl"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<String> r = new ArrayList<>();

		StmtIterator stmti = m.listStatements(null, RDFResourcesTags.dcatDownloadURL, (RDFNode) null);
		while (stmti.hasNext()) {
			r.add(stmti.next().getObject().toString());
		}
		//1,4,5,9,

		for (String s : r) {
			DatasetDB d = saveDataset(s, s);
			String sparqlEndpoint = null;
			if (formatutils.getEquivalentFormat(s).equals(formatutils.DEFAULT_SPARQL))
				sparqlEndpoint = s;
			saveDistribution(s, s, formatutils.getEquivalentFormat(s), d, null, s);
		}

	}

//	public static void main(String[] args) throws FileNotFoundException {
////		new LodStatsMainParser().parse();
//		
//		HashSet<String> idol = new HashSet<>();
//		HashSet<String> lodstats = new HashSet<>();
//
//		new BufferedReader(new FileReader(new File("/home/ciro/lodstats/urls_idol"))).lines().forEach((s) ->{
//			idol.add(s);
//		});
//		
//
//		new BufferedReader(new FileReader(new File("/home/ciro/lodstats/urls_lod"))).lines().forEach((s) ->{
//			lodstats.add(s);
//		});
//		
//		int i = 0;
//		for(String s : lodstats){
//			if(idol.contains(s))
//				i++;
////			else
////				System.out.println(s);
//		}
//		System.out.println(idol.size());
//		System.out.println(lodstats.size());
//		System.out.println(lodstats.size() - i);
//		
//	}

}
