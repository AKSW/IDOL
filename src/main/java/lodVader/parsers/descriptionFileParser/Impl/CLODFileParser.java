package lodVader.parsers.descriptionFileParser.Impl;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
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

import lodVader.enumerators.DistributionStatus;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.interfaces.DescriptionFileParserInterface;
import lodVader.utils.Formats;

public class CLODFileParser implements DescriptionFileParserInterface {

	final static Logger logger = LoggerFactory.getLogger(CLODFileParser.class);

	private Model inModel = ModelFactory.createDefaultModel();

	Property urlProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/url");
	Property formatProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/fileExtension");

	String downloadURLPrefix = "http://download.lodlaundromat.org";

	String URL;
	String format;

	private List<DistributionDB> distributions = new ArrayList<>();
	private List<DatasetDB> datasets = new ArrayList<>();

	/**
	 * Constructor for Class CLODFileParser
	 */
	public CLODFileParser(String URL, String format) {
		this.format = format;
		this.URL = URL;
	}

	public void parse() {

		format = getJenaFormat(format);

		logger.info("Trying to read dataset: " + URL.toString());

		HttpURLConnection URLConnection;
		try {
			URLConnection = (HttpURLConnection) new URL(URL).openConnection();
			URLConnection.setRequestProperty("Accept", "application/rdf+xml");

			inModel.read(URLConnection.getInputStream(), null, format);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		StmtIterator someIterator = inModel.listStatements(null, urlProp, (RDFNode) null);

		while (someIterator.hasNext()) {
			Statement stmt = someIterator.next();

			try {

				String downloadURL = downloadURLPrefix + stmt.getSubject().toString().split("resource")[1];

				String url = stmt.getObject().toString();
				url = url.split("#")[0];

				DatasetDB dataset = new DatasetDB(url);
				dataset.setIsVocabulary(false);
				dataset.setDescriptionFileURL(URL);
				dataset.setTitle(stmt.getObject().toString());
				dataset.update(true);

				DistributionDB distribution = new DistributionDB(url);
				distribution.setDownloadUrl(downloadURL);
				distribution.setTitle(url);
				distribution.setTopDatasetTitle(url);
				distribution.setTopDataset(dataset.getLODVaderID());
				distribution.setIsVocabulary(false);

				ArrayList<Integer> defaultDatasets = new ArrayList<Integer>();
				defaultDatasets.add(dataset.getLODVaderID());
				distribution.setDefaultDatasets(defaultDatasets);

				distribution.setStatus(DistributionStatus.WAITING_TO_STREAM);

				StmtIterator otherIterator = inModel.listStatements(stmt.getSubject().asResource(), formatProp,
						(RDFNode) null);
				try {
					distribution.setFormat(Formats.getEquivalentFormat("nt"));
				} catch (NoSuchElementException e) {
					distribution.setFormat("");
				}
				distribution.update(true, DistributionDB.URI, url);

				ArrayList<Integer> distributionList = new ArrayList<Integer>();
				distributionList.add(distribution.getLODVaderID());
				dataset.setDistributionsIds(defaultDatasets);
				dataset.update(true);

				distributions.add(distribution);
				datasets.add(dataset);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	/**
	 * Get serialization format for Jena processing
	 * 
	 * @param format
	 * @return
	 */
	public String getJenaFormat(String format) {
		format = Formats.getEquivalentFormat(format);
		if (format.equals(Formats.DEFAULT_NTRIPLES))
			return "N-TRIPLES";
		else if (format.equals(Formats.DEFAULT_TURTLE))
			return "TTL";
		else if (format.equals(Formats.DEFAULT_JSONLD))
			return "JSON-LD";
		else
			return "RDF/XML";

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.descriptionfileparser.DescriptionFileParserInterface#
	 * getDistributions()
	 */
	@Override
	public List<DistributionDB> getDistributions() {
		return distributions;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.descriptionfileparser.DescriptionFileParserInterface#
	 * getDatasets()
	 */
	@Override
	public List<DatasetDB> getDatasets() {
		return datasets;
	}

}
