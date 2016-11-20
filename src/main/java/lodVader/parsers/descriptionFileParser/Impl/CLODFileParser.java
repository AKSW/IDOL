package lodVader.parsers.descriptionFileParser.Impl;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionDB.DistributionStatus;
import lodVader.parsers.descriptionFileParser.MetadataParser;
import lodVader.parsers.descriptionFileParser.MetadataParserI;
import lodVader.parsers.descriptionFileParser.helpers.SubsetHelper;
import lodVader.utils.FormatsUtils;
import lodVader.utils.NSUtils;

public class CLODFileParser extends MetadataParser {
	
	String parserName = "CLOD_METADATA_PARSER";

	final static Logger logger = LoggerFactory.getLogger(CLODFileParser.class);

	private Model inModel = ModelFactory.createDefaultModel();

	Property urlProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/url");
	Property formatProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/fileExtension");

	String repositoryAddress = "http://download.lodlaundromat.org";

	String format;

	/**
	 * Constructor for Class CLODFileParser 
	 */
	public CLODFileParser(String repositoryAddress, String format) {
		super("CLOD_METADATA_PARSER");
		this.format = format;
		this.repositoryAddress = repositoryAddress;
	}

	public void parse() {

		FormatsUtils formatsUtils = new FormatsUtils();
		
		format = formatsUtils.getJenaFormat(format);

		logger.info("Trying to read dataset: " + repositoryAddress.toString());

		HttpURLConnection URLConnection;
		try {
			URLConnection = (HttpURLConnection) new URL(repositoryAddress).openConnection();
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
				
				
				DistributionDB distribution = addDistribution(url, false, url, "nt", downloadURL, dataset.getID(), dataset.getTitle(), getParserName(), repositoryAddress);

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
