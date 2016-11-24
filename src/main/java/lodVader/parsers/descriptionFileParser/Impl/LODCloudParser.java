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
import lodVader.parsers.descriptionFileParser.MetadataParser;
import lodVader.parsers.descriptionFileParser.MetadataParserI;
import lodVader.parsers.descriptionFileParser.helpers.LodCloudHelper;
import lodVader.parsers.descriptionFileParser.helpers.SubsetHelper;
import lodVader.streaming.LODVStreamInternetImpl;
import lodVader.utils.FormatsUtils;
import lodVader.utils.FormatsUtils.COMPRESSION_FORMATS;

/**
 * LOV parser. Using LOV api v2 (http://lov.okfn.org/dataset/lov/api/v2/)
 * 
 * @author Ciro Baron Neto
 * 
 *         Sep 27, 2016
 */
public class LODCloudParser extends MetadataParser {

	final static Logger logger = LoggerFactory.getLogger(LODCloudParser.class);


	String repositoryAddress = "http://data.dws.informatik.uni-mannheim.de/lodcloud/2014/ISWC-RDB/datacatalog_metadata.tar.gz";

	/**
	 * Constructor for Class LodCloudParser
	 */
	public LODCloudParser(String dumpAddress) {
		super("LOD_CLOUD_PARSER");
		this.repositoryAddress = dumpAddress;
	}

	/**
	 * Constructor for Class LodCloudParser
	 */
	public LODCloudParser() {
		super("LOD_CLOUD_PARSER");
	}

	/**
	 * Save a LOV Vocabulary or ontology instance the main collection
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

		LODVStreamInternetImpl streamProcessor = new LODVStreamInternetImpl();
		try {
			logger.info("Reading repository: " + repositoryAddress);
			
			streamProcessor.simpleDownload(LODVaderProperties.TMP_FOLDER + "/LingHubFile",
					new URL(repositoryAddress).openStream());
			File f = new File(LODVaderProperties.TMP_FOLDER + "/LingHubFile");

			// if (streamProcessor.getExtension().equals("tar")) {
			InputStream data =

					streamProcessor.checkGZipInputStream(
							new BufferedInputStream(
									streamProcessor.openConnection(repositoryAddress, null).getInputStream()),
							COMPRESSION_FORMATS.GZ);
			logger.info("File extension is tar, creating TarArchiveInputStream and checking compressed files...");

			TarArchiveInputStream tar = new TarArchiveInputStream(data);
			int nf = 0;
			TarArchiveEntry entry = (TarArchiveEntry) tar.getNextTarEntry();
			while (entry != null) {
				if (entry.isFile() && !entry.isDirectory()) {
					String tmpFileName = LODVaderProperties.TMP_FOLDER + entry.getName() + ".tmp";
					streamProcessor.simpleDownload(tmpFileName, tar);
					parseFile(tmpFileName);
					Files.delete(Paths.get(tmpFileName));

				}
				entry = (TarArchiveEntry) tar.getNextEntry();
			}
			// streamProcessor.setExtension(FilenameUtils.getExtension(streamProcessor.getFileName()));
			// }

		} catch (IOException | LODVaderLODGeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		new SubsetHelper().rearrangeSubsets(getDistributions().values(), getDatasets());

	}
}
