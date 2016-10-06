package lodVader.parsers.descriptionFileParser.Impl;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserInterface;
import lodVader.services.mongodb.dataset.DatasetServices;
import lodVader.utils.FormatsUtils;
import lodVader.utils.NSUtils;

public class CLODFileParser implements DescriptionFileParserInterface {

	final static Logger logger = LoggerFactory.getLogger(CLODFileParser.class);

	private Model inModel = ModelFactory.createDefaultModel();

	Property urlProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/url");
	Property formatProp = ResourceFactory.createProperty("http://lodlaundromat.org/ontology/fileExtension");

	String repositoryAddress = "http://download.lodlaundromat.org";


	String URL;
	String format;

	// list of distributions and datasets found in the file
	private List<DistributionDB> distributions = new ArrayList<>();
	private HashMap<String, DatasetDB> datasets = new HashMap<String, DatasetDB>();

	/**
	 * Constructor for Class CLODFileParser
	 */
	public CLODFileParser(String URL, String format) {
		this.format = format;
		this.URL = URL;
	}

	public void parse() {

		FormatsUtils formatsUtils = new FormatsUtils();
		
		format = formatsUtils.getJenaFormat(format);

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

		NSUtils nsUtils = new NSUtils();

		
		while (someIterator.hasNext()) {
			Statement stmt = someIterator.next();

			try {

				String downloadURL = repositoryAddress + stmt.getSubject().toString().split("resource")[1];

				String url = stmt.getObject().toString();
				url = url.split("#")[0];
				
				DatasetDB dataset = new DatasetDB();

				// try to get dataset URI in the format: http://example.org/dataset/
				if(nsUtils.getNS1(url) != null){
					dataset.setUri(nsUtils.getNS1(url));
				}
				else{
					dataset.setUri(nsUtils.getNS0(url));
				}
				
				DatasetDB datasetFind = new DatasetDB(); 
				if(datasetFind.find(true, DatasetDB.URI, dataset.getUri())){
					dataset = datasetFind;
				}
				else{
					dataset.setIsVocabulary(false);
					dataset.setDescriptionFileParser(URL);
					dataset.setTitle(stmt.getObject().toString());
					dataset.update();					
				}
				


				DistributionDB distribution = new DistributionDB();
				distribution.setUri(url);
				distribution.setDownloadUrl(downloadURL);
				distribution.setTitle(url);
				distribution.setTopDatasetTitle(url);
				distribution.setTopDataset(dataset.getID());
				distribution.setIsVocabulary(false);

				ArrayList<String> defaultDatasets = new ArrayList<String>();
				defaultDatasets.add(dataset.getID());
				distribution.setDefaultDatasets(defaultDatasets);

				distribution.setStatus(DistributionStatus.WAITING_TO_STREAM);

				try {
					distribution.setFormat(FormatsUtils.getEquivalentFormat("nt"));
					distribution.setOriginalFormat(FormatsUtils.getEquivalentFormat("nt"));
				} catch (NoSuchElementException e) {
					distribution.setFormat("");
				}
				distribution.update(true, DistributionDB.DOWNLOAD_URL, distribution.getDownloadUrl());

				ArrayList<String> distributionList = new ArrayList<String>();
				distributionList.add(distribution.getID());
				dataset.setDistributionsIds(distributionList);

				distributions.add(distribution);
				datasets.put(dataset.getID(), dataset);
				
				
				dataset.update();

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		organizeDistributionsBasedOnURI();
	}

	/**
	 * Organize datasets distributions by URI. For example, the dataset with the
	 * downloadURL http://example.org/path1/path2 is a subset of the
	 * http://example.org/path1 which is a distribution of the dataset
	 * http://example.org/
	 */
	private void organizeDistributionsBasedOnURI() {

		// sort the distributions by URI
		Collections.sort(distributions, new Comparator<DistributionDB>() {
			public int compare(DistributionDB a, DistributionDB b) {
				return a.getDownloadUrl().compareTo(b.getDownloadUrl());
			}
		});
		
		HashSet<String> removeSet = new HashSet<String>();
		

		DatasetDB tmpDataset = null;
		for (DistributionDB distribution : distributions) {
			if (tmpDataset == null) {
				tmpDataset = datasets.get(distribution.getTopDatasetID());
			} else {
				if (distribution.getUri().startsWith(tmpDataset.getUri())) {
					try {
						tmpDataset.addDistributionID(distribution.getID());
						tmpDataset.update();

//						removeSet.add(tmpDataset.getID());

						distribution.setTopDataset(tmpDataset.getID());
						distribution.update();
						
					} catch (LODVaderMissingPropertiesException e) {
						e.printStackTrace();
					}

				} else {
					tmpDataset.find(true, DatasetDB.ID, distribution.getTopDatasetID());
				}
			}
		}
		
		// remove all datasets that don't contain distributions
		for(String i : removeSet){
			new DatasetServices().removeDataset(i);
			datasets.remove(i);
		}
		
		
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
		return new ArrayList<DatasetDB>(datasets.values());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getParserName(
	 * )
	 */
	@Override
	public String getParserName() {
		return "CLOD_METADATA_PARSER";
	}
	
	
	/* (non-Javadoc)
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#getRepositoryAddress()
	 */
	@Override
	public String getRepositoryAddress() {
		return repositoryAddress;
	}

}
