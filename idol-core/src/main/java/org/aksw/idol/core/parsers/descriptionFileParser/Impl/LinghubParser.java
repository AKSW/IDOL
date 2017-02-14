/**
 * 
 */
package org.aksw.idol.core.parsers.descriptionFileParser.Impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import org.aksw.idol.core.loader.LODVaderProperties;
import org.aksw.idol.core.mongodb.collections.DatasetDB;
import org.aksw.idol.core.mongodb.collections.DistributionDB;
import org.aksw.idol.core.parsers.descriptionFileParser.MetadataParser;
import org.aksw.idol.core.parsers.descriptionFileParser.helpers.LodCloudHelper;
import org.aksw.idol.core.parsers.descriptionFileParser.helpers.SubsetHelper;
import org.aksw.idol.core.streaming.LODVStreamInternetImpl;
import org.aksw.idol.utils.FormatsUtils;
import org.aksw.idol.utils.FormatsUtils.COMPRESSION_FORMATS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * Linghub parser
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class LinghubParser extends MetadataParser{

	final static Logger logger = LoggerFactory.getLogger(LinghubParser.class);

	// String repositoryAddress =
	// "http://cirola2000.cloudapp.net/files/linghub.nt.gz";
	// String repositoryAddress = "http://localhost/dbpedia/linghub.nt.gz";
	String repositoryAddress;

	/**
	 * Constructor for Class LodCloudParser 
	 */
	public LinghubParser(String repositoryAddress) {
		super("LINGHUB_PARSER");
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
	public DistributionDB saveDistribution(String url, String title, String format, DatasetDB datasetDB) {
		
		return addDistribution(url, false, title, format, url, datasetDB.getID(), datasetDB.getTitle(), getParserName(), repositoryAddress, null, null);

	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see idol.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {

		LODVStreamInternetImpl streamProcessor = new LODVStreamInternetImpl();
		try {
			streamProcessor.simpleDownload(LODVaderProperties.TMP_FOLDER + "/LingHubFile",
					new URL(repositoryAddress).openStream());
			File f = new File(LODVaderProperties.TMP_FOLDER + "/LingHubFile");

			Model model = ModelFactory.createDefaultModel(); 
			model.read(streamProcessor.checkGZipInputStream(new FileInputStream(f), COMPRESSION_FORMATS.GZ), null,
					new FormatsUtils().getJenaFormat("nt"));
			f.delete();
			LodCloudHelper helper = new LodCloudHelper(model); 
			for (String dataset : helper.getDatasets()) {
				DatasetDB datasetDB = null;
				for (RDFNode distribution : helper.getDistributions(dataset)) {
					if (!helper.getFormat2(distribution).equals("")) {
						System.out.println(helper.getFormat2(distribution));
						
						
						if (datasetDB == null)
							datasetDB = saveDataset(dataset, helper.getTitle(dataset));

						DistributionDB distributionDB = saveDistribution(helper.getAccessURL(distribution),
								helper.getTitle(dataset), helper.getFormat2(distribution), datasetDB);
						distributionDB.addDefaultDatasets(datasetDB.getID());
						datasetDB.addDistributionID(distributionDB.getID());
						try {
							distributionDB.update();
							datasetDB.update();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		new SubsetHelper().rearrangeSubsets(getDistributions().values(), getDatasets());

	}


}