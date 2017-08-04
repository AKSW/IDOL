package org.aksw.idol.file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;

public class FileStatementRDF {

	public FileStatementRDF() {

	}

//	public static void main(String[] args) throws IOException {
//
//		URL documentUrl = new URL("http://downloads.dbpedia.org/2016-10/core-i18n/ca/freebase_links_ca.ttl.bz2");
//		BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(documentUrl.openStream());
//
//		RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
//
//		org.openrdf.model.Graph myGraph = new org.openrdf.model.impl.GraphImpl();
//		StatementCollector collector = new StatementCollector(myGraph);
//		rdfParser.setRDFHandler(collector);
//		try {
//			rdfParser.parse(inputStream, documentUrl.toString());
//		} catch (RDFParseException | RDFHandlerException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//
//		FileOutputStream out = new FileOutputStream("/tmp/file.rdf");
//		RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
//		try {
//			writer.startRDF();
//			for (Statement st : myGraph) {
//				writer.handleStatement(st);
//				System.out.println(st.getSubject().toString());
//				
//			}
//			writer.endRDF();
//		} catch (RDFHandlerException e) {
//			// oh no, do something!
//		}
//	}

}
