package org.aksw.idol.utils;

import java.util.ArrayList;

import org.apache.commons.io.FilenameUtils;

public class FormatsUtils {

	public static final String DEFAULT_TURTLE = "ttl";

	public static final String DEFAULT_NTRIPLES = "nt";

	public static final String DEFAULT_NQUADS = "nq";

	public static final String DEFAULT_N3 = "n3";

	public static final String DEFAULT_TQL = "tql";

	public static final String DEFAULT_RDFXML = "rdf";

	public static final String DEFAULT_JSONLD = "jsonld";

	public static final String DEFAULT_SPARQL = "sparql";
	 
	
	public static enum COMPRESSION_FORMATS {ZIP, TAR, GZ, TAR_GZ, TGZ, BZ2, NO_COMPRESSION} ;


	public static String getEquivalentFormat(String str) {
		if(str.length() > 30)
		str = str.substring(str.length() - 30);
		String soriginal = str;

		if (TURTLE_FORMATS.contains(str) || str.contains("ttl") || str.contains("turtle"))
			return DEFAULT_TURTLE;
		else if (NTRIPLES_FORMATS.contains(str))
			return DEFAULT_NTRIPLES;
		else if (RDFXML_FORMATS.contains(str) || str.contains("rdf") || str.contains("RDF"))
			return DEFAULT_RDFXML;
		else if (NQUADS_FORMATS.contains(str)|| str.contains("nq"))
			return DEFAULT_NQUADS;
		else if (SPARQL_FORMATS.contains(str)|| str.contains("sparql"))
			return DEFAULT_SPARQL;
		else if (N3_FORMATS.contains(str)|| str.contains("n3"))
			return DEFAULT_N3;
		else if (JSONLD_FORMATS.contains(str))
			return DEFAULT_JSONLD;
		else if (TQL_FORMATS.contains(str))
			return DEFAULT_TQL;
		
		str = FilenameUtils.getExtension(str);
		
		
		if (TURTLE_FORMATS.contains(str) || str.contains("ttl") || str.contains("turtle"))
			return DEFAULT_TURTLE;
		else if (NTRIPLES_FORMATS.contains(str))
			return DEFAULT_NTRIPLES;
		else if (RDFXML_FORMATS.contains(str) || str.contains("rdf") || str.contains("RDF"))
			return DEFAULT_RDFXML;
		else if (NQUADS_FORMATS.contains(str)|| str.contains("nq"))
			return DEFAULT_NQUADS;
		else if (SPARQL_FORMATS.contains(str)|| str.contains("sparql"))
			return DEFAULT_SPARQL;
		else if (N3_FORMATS.contains(str)|| str.contains("n3"))
			return DEFAULT_N3;
		else if (JSONLD_FORMATS.contains(str))
			return DEFAULT_JSONLD;
		else if (TQL_FORMATS.contains(str))
			return DEFAULT_TQL;
		
		str = FilenameUtils.getExtension(soriginal.replace("."+str, ""));
		
		
		if (TURTLE_FORMATS.contains(str) || str.contains("ttl") || str.contains("turtle"))
			return DEFAULT_TURTLE;
		else if (NTRIPLES_FORMATS.contains(str))
			return DEFAULT_NTRIPLES;
		else if (RDFXML_FORMATS.contains(str) || str.contains("rdf") || str.contains("RDF"))
			return DEFAULT_RDFXML;
		else if (NQUADS_FORMATS.contains(str)|| str.contains("nq"))
			return DEFAULT_NQUADS;
		else if (SPARQL_FORMATS.contains(str)|| str.contains("sparql"))
			return DEFAULT_SPARQL;
		else if (N3_FORMATS.contains(str)|| str.contains("n3"))
			return DEFAULT_N3;
		else if (JSONLD_FORMATS.contains(str))
			return DEFAULT_JSONLD;
		else if (TQL_FORMATS.contains(str))
			return DEFAULT_TQL;
		else {
			return "";
		}
	}
	

	private static final ArrayList<String> N3_FORMATS = new ArrayList<String>() {
		{
			add("n3");
			add("rdf/n3");
			add("text/n3");
		}
	};

	private static final ArrayList<String> JSONLD_FORMATS = new ArrayList<String>() {
		{
			add("jsonld");
			add("json-ld");
			add("JSONLD");
			add("JSONld");
			add("JSON-LD");

		}
	};

	private static final ArrayList<String> SPARQL_FORMATS = new ArrayList<String>() {
		{
			add("api/sparql");
			add("sparql");
			add("SPARQL");
		}
	};

	private static final ArrayList<String> TQL_FORMATS = new ArrayList<String>() {
		{
			add("tql");
			add("TQL");
		}
	};

	private static final ArrayList<String> NQUADS_FORMATS = new ArrayList<String>() {
		{
			add("x-nquads");
			add("application/x-nquads");
			add("nq");
			add("NQ");
			add("nquads");
			add("nquad");
			add("n quads");
			add("n-quads");
			add("gz:nq");
		}
	};
	private static final ArrayList<String> TURTLE_FORMATS = new ArrayList<String>() {
		{
			add("ttl");
			add("TTL");
			add("turtle");
			add("TURTLE");
			add("meta/void");
			add("meta/rdf-schema");
			add("text/turtle");
			add("example/turtle");
			add("example/x-turtle");
			add("rdf-turtle");
			add("rdf/turtle");
			add("7z:ttl");
		}
	};
	
	private static final ArrayList<String> NTRIPLES_FORMATS = new ArrayList<String>() {
		{
			add("nt");
			add("NT");
			add("application/x-ntriples");
			add("application/n-ntriples");
			add("example/ntriples");
			add("example/n3");
			add("ntriples");
			add("NTRIPLES");
			add("n-triples");
			add("text/ntriples");
			add("rdf/nt");
			add("gzip:ntriples");
			add("bz2:nt");
			add("gz:nt");
		}
	};
	private static final ArrayList<String> RDFXML_FORMATS = new ArrayList<String>() {
		{
			add("application/rdf+xml");
			add("application/rdf xml");
			add("application/xhtml+xml");
			add("rdf");
			add("RDF");
			add("RDFXML");
			add("example/rdf+xml");
			add("example/rdf");
			add("rdf+xml");
			add("rdf+xml ");
			add("rdf xml");
			add("rdfxml");
		}
	};


	/**
	 * Return the file compression format based on the URL
	 * @param url
	 * @return the compression format
	 */
	public COMPRESSION_FORMATS getCompressionFormat(String url){
		if(url.endsWith(".zip")){
			return COMPRESSION_FORMATS.ZIP;
		}
		else if(url.endsWith(".tar")){
			return COMPRESSION_FORMATS.TAR;
		}
		else if(url.endsWith(".tar.gz")){
			return  COMPRESSION_FORMATS.TAR_GZ;			
		}
		else if(url.endsWith(".bz2")){
			return COMPRESSION_FORMATS.BZ2;			
		}
		else if(url.endsWith(".gz")){
			return COMPRESSION_FORMATS.GZ;			
		}
		else if(url.endsWith(".tgz")){
			return COMPRESSION_FORMATS.TGZ;						
		}
		else return COMPRESSION_FORMATS.NO_COMPRESSION;
	}
	
	
	/**
	 * Get serialization format for Jena processing
	 * 
	 * @param format
	 * @return
	 */
	public String getJenaFormat(String format) {
		format = FormatsUtils.getEquivalentFormat(format);
		if (format.equals(FormatsUtils.DEFAULT_NTRIPLES) || format.contains("nt"))
			return "N-TRIPLES";
		else if (format.equals(FormatsUtils.DEFAULT_TURTLE) || format.contains("ttl") || format.contains("turtle"))
			return "TTL";
		else if (format.equals(FormatsUtils.DEFAULT_JSONLD))
			return "JSON-LD";
		else if (format.equals(FormatsUtils.DEFAULT_RDFXML) || format.contains("rdf"))
			return "RDF/XML";
		else
			return "";

	}

}
