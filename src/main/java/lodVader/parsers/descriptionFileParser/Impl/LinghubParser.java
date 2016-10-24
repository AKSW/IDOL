/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionDB.DistributionStatus;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserInterface;
import lodVader.parsers.descriptionFileParser.helpers.LodCloudHelper;
import lodVader.streaming.LODVaderCoreStream;
import lodVader.utils.FormatsUtils;

/**
 * LOV parser. Using LOV api v2 (http://lov.okfn.org/dataset/lov/api/v2/)
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class LinghubParser implements DescriptionFileParserInterface {

	final static Logger logger = LoggerFactory.getLogger(LinghubParser.class);

	HashMap<String, DistributionDB> distributions = new HashMap<String, DistributionDB>();

	HashMap<String, DatasetDB> datasets = new HashMap<String, DatasetDB>();

	String repositoryAddress = "http://linghub.lider-project.eu/linghub.nt.gz";

	/**
	 * Constructor for Class LodCloudParser
	 */
	public LinghubParser(String dumpAddress) {
		repositoryAddress = dumpAddress;
	}

	/**
	 * Constructor for Class LodCloudParser
	 */
	public LinghubParser() {
	}

	/**
	 * Save a LOV Vocabulary or ontology instance the main collection
	 * 
	 * @param the
	 *            CkanDataset
	 * @return the DatasetDB instance
	 */
	public DatasetDB saveDataset(String url, String title) {

		DatasetDB datasetDB = new DatasetDB(url);
		datasetDB.setIsVocabulary(true);
		datasetDB.setTitle(title);
		datasetDB.setLabel(title);
		datasetDB.setDescriptionFileParser(getParserName());
		datasetDB.addProvenance(repositoryAddress);
		try {
			datasetDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		datasets.put(datasetDB.getUri(), datasetDB);

		return datasetDB;
	}

	/**
	 * Save a CkanResource instance the main collection (a distribution)
	 * 
	 * @param the
	 *            CkanResource
	 * @return the DistributionDB instance
	 */
	public DistributionDB saveDistribution(String url, String title, String format, DatasetDB datasetDB) {

		DistributionDB distributionDB = new DistributionDB(url);
		distributionDB.setTitle(title);
		distributionDB.setUri(url);
		distributionDB.setIsVocabulary(true);
		distributionDB.setTopDataset(datasetDB.getID());
		distributionDB.setTopDatasetTitle(datasetDB.getTitle());
		if (distributionDB.getID() == null)
			distributionDB.setStatus(DistributionStatus.WAITING_TO_STREAM);
		distributionDB.setFormat(FormatsUtils.getEquivalentFormat(format));
		distributionDB.setOriginalFormat(format);
		try {
			distributionDB.update();
		} catch (LODVaderMissingPropertiesException e) {
			e.printStackTrace();
		}
		distributions.put(distributionDB.getUri(), distributionDB);

		return distributionDB;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#
	 * getDistributions()
	 */
	@Override
	public List<DistributionDB> getDistributions() {
		return new ArrayList<DistributionDB>(distributions.values());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.parsers.interfaces.DescriptionFileParserInterface#getDatasets()
	 */
	@Override
	public List<DatasetDB> getDatasets() {
		return new ArrayList<DatasetDB>(datasets.values());
	}

	public void parseFile(String file) {
		Model model = ModelFactory.createDefaultModel();
		try {
			model.read((InputStream) new FileInputStream(new File(file)), "RDFXML");
			LodCloudHelper helper = new LodCloudHelper(model);
			for (String dataset : helper.getDatasets()) {
				DatasetDB datasetDB = saveDataset(dataset, helper.getTitle(dataset));
				for (RDFNode distribution : helper.getDistributions(dataset)) {
					DistributionDB distributionDB = saveDistribution(helper.getAccessURL(distribution),
							helper.getTitle(dataset), helper.getFormat(distribution), datasetDB);
					distributionDB.addDefaultDatasets(datasetDB.getID());
					datasetDB.addDistributionID(distributionDB.getID());
					try {
						distributionDB.update();
						datasetDB.update();
					} catch (LODVaderMissingPropertiesException e) {
						e.printStackTrace();
					}
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#parse()
	 */
	@Override
	public void parse() {

		LODVaderCoreStream streamProcessor = new LODVaderCoreStream();
		try {
			streamProcessor.downloadUrl = new URL(repositoryAddress);
			streamProcessor.RDFFormat = "";
			streamProcessor.openConnection();
			streamProcessor.checkGZipInputStream();

			Model model = ModelFactory.createDefaultModel();
			model.read((streamProcessor.inputStream), null, new FormatsUtils().getJenaFormat("nt"));
			LodCloudHelper helper = new LodCloudHelper(model);
			for (String dataset : helper.getDatasets()) {
				DatasetDB datasetDB = saveDataset(dataset, helper.getTitle(dataset));
				for (RDFNode distribution : helper.getDistributions(dataset)) {
					DistributionDB distributionDB = saveDistribution(helper.getAccessURL(distribution),
							helper.getTitle(dataset), helper.getFormat(distribution), datasetDB);
					distributionDB.addDefaultDatasets(datasetDB.getID());
					datasetDB.addDistributionID(distributionDB.getID());
					try {
						distributionDB.update();
						datasetDB.update();
					} catch (LODVaderMissingPropertiesException e) {
						e.printStackTrace();
					}
				}
			}

		} catch (IOException | LODVaderLODGeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
		return "LINGHUB_PARSER";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.interfaces.DescriptionFileParserInterface#
	 * getRepositoryAddress()
	 */
	@Override
	public String getRepositoryAddress() {
		return repositoryAddress;
	}

}
