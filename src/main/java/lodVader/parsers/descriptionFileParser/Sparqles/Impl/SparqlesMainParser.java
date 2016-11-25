/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Sparqles.Impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.parsers.descriptionFileParser.MetadataParser;
import lodVader.parsers.descriptionFileParser.Sparqles.Impl.SparqlesAPIEndpoint.Dataset;

/**
 * Linghub parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class SparqlesMainParser extends MetadataParser {

	final static Logger logger = LoggerFactory.getLogger(SparqlesMainParser.class);

	String repositoryAddress;
	
	int threads = 20;

	/**
	 * Constructor for Class LodCloudParser
	 */
	public SparqlesMainParser(String repositoryAddress) {
		super("SPARQLES_PARSER");
		this.repositoryAddress = repositoryAddress;
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

		Collection<SparqlesAPIEndpoint> list = new JsonSparqlesAPIConverter(repositoryAddress).getList();

		ExecutorService ex = Executors.newFixedThreadPool(threads);

		for (SparqlesAPIEndpoint e : list) {

			Runnable r = () -> {

				logger.info("* * * * * " + e.uri + " * * * * ");
				List<Dataset> datasets = new ArrayList<>(e.datasets);

				DatasetDB dataset = null;
				try {
					dataset = saveDataset(e.uri, e.uri);
					List<String> distributions = new ArrayList<>(new SparqlesHelper().getDistributions(e.uri));
					// if (go)
					if (distributions.size() > 0) {

						for (String graph : distributions) {
							String queryPart = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + graph + "> { ?s ?p ?o } }";
							String uri = e.uri + "?query=" + queryPart;
							// URLEncoder.encode(
							if (!uri.contains("openlink"))
								saveDistribution(uri, uri, "sparql", dataset, graph, e.uri);
						}
					}

				} catch (IOException e1) {
					saveDistribution(e.uri, e.uri, "sparql", dataset, null, e.uri);
					System.out.println("Error: " + e1.getMessage());
				}

				catch (NullPointerException e2) {
					// TODO: handle exception
				}

			};
			
			ex.execute(r);
		}
		
		ex.shutdown();
		
		try {
			ex.awaitTermination(10, TimeUnit.DAYS);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}
