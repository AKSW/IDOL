/**
 * 
 */
package org.aksw.idol.application;

import java.io.File;
import java.text.DecimalFormat;

import org.aksw.idol.bloomfilters.BloomFilterI;
import org.aksw.idol.bloomfilters.impl.BloomFilterFactory;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;
import org.aksw.idol.services.mongodb.MetadataParserServices;
import org.aksw.idol.utils.BloomFilterCache;
import org.aksw.idol.utils.FileStatement;
import org.openrdf.model.Statement;
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

	int bfSize = 75_000 ;

	long limit = 75_000 ;

	boolean keepProcessing = false;

	String fileName = null;

	FileStatement fileStatement1 = null;

	FileStatement fileStatement2 = null;

//	BloomFilterCache bf = new BloomFilterCache(bfSize, 0.000_001);
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

		fileStatement1 = new FileStatement(LODVaderProperties.TMP_FOLDER, fileName + 1);
		fileStatement2 = new FileStatement(LODVaderProperties.TMP_FOLDER, fileName + 2);

	}

	public void count() {

		logger.info("Counting uniq triples for parser: " + parser.getParserName());

		for (FileStatement f : new MetadataParserServices().getFilesFromParser(parser.getParserName())) {
			try {
				while (f.hasNext()) {
					processStatement(f.getStatement(), fileStatement1);
					totalTriples++;
					if (totalTriples % msgInterval == 0) {
						logger.info(formatter.format(totalTriples) + " statements processed ("+uniq+" unique).");
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
//			bf = new BloomFilterCache(bfSize, 0.000_01);
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

			new File( LODVaderProperties.TMP_FOLDER + fileName + 2)
					.renameTo(new File( LODVaderProperties.TMP_FOLDER + fileName + 1));
			fileStatement2.clear();

			fileStatement2 = new FileStatement( LODVaderProperties.TMP_FOLDER, fileName + 2);

		}

		updateCounter(totalUniq, totalTriples);

		fileStatement1.close();

		logger.info("uniq " + totalUniq);
		logger.info("triples " + totalTriples);
		
		new File(LODVaderProperties.TMP_FOLDER, fileName + 1).delete();
		new File(LODVaderProperties.TMP_FOLDER, fileName + 2).delete();
	}

	private void processStatement(Statement s, FileStatement fileStatement) {
		String triple = s.getSubject().stringValue() + " " + s.getPredicate().stringValue() + " "
				+ s.getObject().stringValue();
		if (!bf.compare(triple)) {
			if (uniq > limit) {
				fileStatement.writeStatement(s);
				triplesInFile++;
				if (triplesInFile % msgInterval == 0) {
					logger.info(triplesInFile + " statements saved into file.");
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
