/**
 * 
 */
package org.aksw.idol.application;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collection;

import org.aksw.idol.bloomfilters.BloomFilterI;
import org.aksw.idol.bloomfilters.impl.BloomFilterFactory;
import org.aksw.idol.file.FileStatementCustom;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;
import org.aksw.idol.services.mongodb.MetadataParserServices;
import org.aksw.idol.streaming.LODVStreamInternetImpl;
import org.aksw.idol.tupleManager.processors.BasicProcessorInterface;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 19, 2016
 */
public class DatasourcesUniqTriples {

	final static Logger logger = LoggerFactory.getLogger(DatasourcesUniqTriples.class);

	MetadataParser parser = null;

	int bfSize = 990_000_000; 

	long limit = 990_000_000;

	boolean keepProcessing = false;

	String fileName = null;

	FileStatementCustom fileStatement1 = null;

	FileStatementCustom fileStatement2 = null;

	// BloomFilterCache bf = new BloomFilterCache(bfSize, 0.000_001);
	BloomFilterI bf = BloomFilterFactory.newBloomFilter();

	long uniq = 0;

	long total = 0;

	long totalUniq = 0;

	long totalTriples = 0;

	long triplesInFile = 0;

	int msgInterval = 1_000_000;

	DecimalFormat formatter = new DecimalFormat("#,###,###,###,###");

	/**
	 * Constructor for Class DatasourcesUniqTriples
	 */
	public DatasourcesUniqTriples(MetadataParser parser) {
		this.parser = parser;

		bf.create(bfSize, 0.000_001);
		fileName = "uniqExccess";

		new File(LODVaderProperties.TMP_FOLDER, fileName + 1).delete();
		new File(LODVaderProperties.TMP_FOLDER, fileName + 2).delete();

		fileStatement1 = new FileStatementCustom(LODVaderProperties.TMP_FOLDER, fileName + 1);
		fileStatement2 = new FileStatementCustom(LODVaderProperties.TMP_FOLDER, fileName + 2);

	}

	class CountProcessor implements BasicProcessorInterface {

		@Override
		public void process(Statement st) {
			processStatement(st, fileStatement1);
			totalTriples++;
			if (totalTriples % msgInterval == 0) {
				logger.info(formatter.format(totalTriples) + " statements processed (" + uniq + " unique).");
			}
		}

	}

	public void countLoadingFromInternet() {

		logger.info("Counting uniq triples (streaming from the internet) for parser: " + parser.getParserName());

		LODVStreamInternetImpl stream = new LODVStreamInternetImpl();
		stream.getPipelineProcessor().registerProcessor(new CountProcessor());

		Collection<DistributionDB> distributions = new MetadataParserServices()
				.getDistributionsFromParser(parser.getParserName());

		logger.info("Streaming " + distributions.size() + " distributions");

		for (DistributionDB d : distributions) {
			try {
				stream.streamAndParse(d.getDownloadUrl(), d.getFormat());

			} catch (RDFParseException | RDFHandlerException | IOException e) {
				e.printStackTrace();
			}
		}
		while (keepProcessing) {
			logger.info("" );
			logger.info("Reading file: " + LODVaderProperties.TMP_FOLDER + fileName + 1);
			logger.info("Triples on file: " + formatter.format(triplesInFile));
			logger.info("" );
			// bf = new BloomFilterCache(bfSize, 0.000_01);
			bf = BloomFilterFactory.newBloomFilter();
			bf.create(bfSize, 0.000_001);
			triplesInFile = 0;
			uniq = 0;
			total = 0;
			fileStatement1.close();
			while (fileStatement1.hasNext()) {
				processStatement(fileStatement1.getStatement(), fileStatement2);
				total++;
			}
			fileStatement2.close();
			fileStatement1.close();

			new File(LODVaderProperties.TMP_FOLDER + fileName + 2)
					.renameTo(new File(LODVaderProperties.TMP_FOLDER + fileName + 1));
			fileStatement2.clear();

			fileStatement2 = new FileStatementCustom(LODVaderProperties.TMP_FOLDER, fileName + 2);

		}

		updateCounter(totalUniq, totalTriples);

		fileStatement1.close();

		logger.info("uniq " + totalUniq);
		logger.info("triples " + totalTriples);

		new File(LODVaderProperties.TMP_FOLDER, fileName + 1).delete();
		new File(LODVaderProperties.TMP_FOLDER, fileName + 2).delete();

	}

	public void countLoadingFile() {

		logger.info("Counting uniq triples for parser: " + parser.getParserName());

		for (FileStatementCustom f : new MetadataParserServices().getFilesFromParser(parser.getParserName())) {
			try {
				while (f.hasNext()) {
					processStatement(f.getStatement(), fileStatement1);
					totalTriples++;
					if (totalTriples % msgInterval == 0) {
						logger.info(formatter.format(totalTriples) + " statements processed (" + uniq + " unique).");
					}
				}
				f.close();
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}

		while (keepProcessing) {
			logger.info("Reading file: " + LODVaderProperties.TMP_FOLDER + fileName + 1);
			logger.info("Triples on file: " + formatter.format(triplesInFile));
			// bf = new BloomFilterCache(bfSize, 0.000_01);
			bf = BloomFilterFactory.newBloomFilter();
			bf.create(bfSize, 0.000_001);
			triplesInFile = 0;
			uniq = 0;
			total = 0;
			fileStatement1.close();
			while (fileStatement1.hasNext()) {
				processStatement(fileStatement1.getStatement(), fileStatement2);
				total++;
			}
			fileStatement2.close();
			fileStatement1.close();

			new File(LODVaderProperties.TMP_FOLDER + fileName + 2)
					.renameTo(new File(LODVaderProperties.TMP_FOLDER + fileName + 1));
			fileStatement2.clear();

			fileStatement2 = new FileStatementCustom(LODVaderProperties.TMP_FOLDER, fileName + 2);

		}

		updateCounter(totalUniq, totalTriples);

		fileStatement1.close();

		logger.info("uniq " + totalUniq);
		logger.info("triples " + totalTriples);

		new File(LODVaderProperties.TMP_FOLDER, fileName + 1).delete();
		new File(LODVaderProperties.TMP_FOLDER, fileName + 2).delete();
	}

	private void processStatement(Statement s, FileStatementCustom fileStatement) {
		String triple = s.getSubject().stringValue() + " " + s.getPredicate().stringValue() + " "
				+ s.getObject().stringValue();
		if (!bf.compare(triple)) {
			if (uniq > limit) {
				fileStatement.writeStatement(s);
				triplesInFile++;
				if (triplesInFile % msgInterval == 0) {
					logger.info(triplesInFile + " statements saved into file!!!!!!!!!!!!");
				}
				keepProcessing = true;
			} else {
				bf.add(triple);
				uniq++;
				keepProcessing = false;
				totalUniq++;
			}
		}
	}

	public void updateCounter(long uniq, long total) {
		MetadataParserServices services = new MetadataParserServices();
		services.updateTriples(parser, uniq, total);
	}

}
