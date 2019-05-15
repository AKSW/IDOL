package org.aksw.idol.parsers.descriptionFileParser.Impl;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.aksw.idol.mongodb.collections.DatasetDB;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;
import org.aksw.idol.parsers.descriptionFileParser.helpers.SubsetHelper;
import org.aksw.idol.utils.FormatsUtils;
import org.aksw.idol.utils.NSUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CLODParser extends MetadataParser {
	
	String parserName = "CLOD_METADATA_PARSER";

	final static Logger logger = LoggerFactory.getLogger(CLODParser.class);

	private Model inModel = ModelFactory.createDefaultModel();

	Property urlProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/url");
	Property formatProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/fileExtension");

	String repositoryAddress = "http://download.lodlaundromat.org";
	String repositoryAddress2 = "";

	String format;

	/**
	 * Constructor for Class CLODFileParser 
	 */
	public CLODParser(String repositoryAddress, String format) {
		super("CLOD_METADATA_PARSER");
		this.format = format;
		this.repositoryAddress2 = repositoryAddress;
	}

	public void parse() {

		FormatsUtils formatsUtils = new FormatsUtils();
		
		format = formatsUtils.getJenaFormat(format);

		logger.info("Trying to read dataset: " + repositoryAddress2.toString());

		HttpURLConnection URLConnection;
		try {
			URLConnection = (HttpURLConnection) new URL(repositoryAddress2).openConnection();
			URLConnection.setRequestProperty("Accept", "application/rdf+xml");

			inModel.read(URLConnection.getInputStream(), null, format);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		StmtIterator someIterator = inModel.listStatements(null, urlProp, (RDFNode) null);

		NSUtils nsUtils = new NSUtils();

		
		while (someIterator.hasNext()) {
			Statement stmt = someIterator.next();

			try {

				String downloadURL = repositoryAddress + stmt.getSubject().toString().split("resource")[1];
				String datasetURI;
				String datasetTitle;

				String url = stmt.getObject().toString();
				url = url.split("#")[0];
				
				// try to get dataset URI in the format: http://example.org/dataset/
				if(nsUtils.getNS1(url) != null){
					datasetURI = nsUtils.getNS1(url);
				}
				else{
					datasetURI = nsUtils.getNS0(url);
				}
				
				datasetTitle = stmt.getObject().toString();
				

				DatasetDB dataset  = addDataset(datasetURI, false, datasetTitle, datasetTitle, getParserName());
				
				
				DistributionDB distribution = addDistribution(url, false, url, "nt", downloadURL, dataset.getID(),
						dataset.getTitle(), getParserName(), repositoryAddress, null, null);

				addDistribution(distribution);
				addDataset(dataset);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		new SubsetHelper().rearrangeSubsets(getDistributions().values(), getDatasets());
	}
}
