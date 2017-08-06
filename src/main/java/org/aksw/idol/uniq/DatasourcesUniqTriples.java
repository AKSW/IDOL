/**
 * 
 */
package org.aksw.idol.uniq;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collection;

import org.aksw.idol.comparator.ComparatorI;
import org.aksw.idol.comparator.bloomfilters.impl.ComparatorFactory;
import org.aksw.idol.file.FileCacheStatement;
import org.aksw.idol.file.FileStatementCustom;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;
import org.aksw.idol.properties.CalculateUniqPerDataSourceProperties;
import org.aksw.idol.properties.Properties;
import org.aksw.idol.services.mongodb.MetadataParserServices;
import org.aksw.idol.streaming.LODVStreamInternetImpl;
import org.aksw.idol.tupleManager.processors.BasicProcessorInterface;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 19, 2016
 */
@Component
public class DatasourcesUniqTriples {
	
	final static Logger logger = LoggerFactory.getLogger(DatasourcesUniqTriples.class);

	MetadataParser parser = null;

	long bfSize;
	
	long limit;
	
	double bfFpp;
	
	String tmpFolder;
	
	@Autowired
	public DatasourcesUniqTriples(Properties properties, FileCacheStatement fileStatement1, FileCacheStatement fileStatement2) {
		this.bfSize = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource().getBloomFilterSize();
		this.limit = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource().getBloomFilterSize();
		this.bfFpp = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource().getBloomFilterFpp();
		this.tmpFolder = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource().getTmpDir();
		this.fileStatement1 = fileStatement1;
		this.fileStatement2 = fileStatement2;
	}
	
	
	boolean keepProcessing = false;

	String fileName = "uniqExccess";
	
	FileCacheStatement fileStatement1;
	
	FileCacheStatement fileStatement2;

	ComparatorI comparator = ComparatorFactory.newComparator();

	long uniq = 0;

	long total = 0;

	long totalUniq = 0;

	long totalTriples = 0;

	long triplesInFile = 0;

	int msgInterval = 1_000_000;

	DecimalFormat formatter = new DecimalFormat("#,###,###,###,###");

	public void setup(MetadataParser parser) {
		this.parser = parser;

		comparator.create(bfSize, bfFpp);
		
		deleteTmpFileIfExist();

		try {
			fileStatement1.openWriter();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			countLoadingFromInternet();
		} catch (IOException e) {
			e.printStackTrace();
		}
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

	public void countLoadingFromInternet() throws IOException {

		logger.info("Counting uniq triples (streaming from the internet) for parser: " + parser.getParserName());

		LODVStreamInternetImpl stream = new LODVStreamInternetImpl();
		stream.getPipelineProcessor().registerProcessor(new CountProcessor());

		Collection<DistributionDB> distributions = new MetadataParserServices()
				.getDistributionsFromParser(parser.getParserName());

		logger.info("Streaming " + distributions.size() + " distributions");

		int c = 0;
		
		for (DistributionDB d : distributions) {
			if(c<150000)
			try {
				c++;
				stream.streamAndParse(d.getDownloadUrl(), d.getFormat());

			} catch (RDFParseException | RDFHandlerException | IOException e) {
				e.printStackTrace();
			}
			logger.info("Comparator size: "+ comparator.getNumberOfElements());
		}
				
		// after finishing reading all triples, close the cache file
		fileStatement1.closeWriter();
		countFromFile();
	}
	
	public void countFromFile() throws IOException  {

		while (keepProcessing) {
			
			logger.info("" );
			logger.info("Reading file: " + LODVaderProperties.TMP_FOLDER + fileName + 1);
			logger.info("Triples on file: " + formatter.format(triplesInFile));
			logger.info("" );
			
			comparator = ComparatorFactory.newComparator();
			comparator.create(bfSize, bfFpp);
			triplesInFile = 0;
			uniq = 0;
			total = 0;
			
			fileStatement2.createNewFile();
			fileStatement2.openWriter();

			fileStatement1.openReader();
			
			while (fileStatement1.hasNext()) {
				processStatement(fileStatement1.getStatement(), fileStatement2);
				total++;
			}
			fileStatement2.closeWriter();
			
			fileStatement1.removeFile();
			fileStatement1.fileName = fileStatement2.fileName;
		}

		updateCounter(totalUniq, totalTriples);

		fileStatement1.removeFile();

		logger.info("uniq " + totalUniq);
		logger.info("triples " + totalTriples);
		

	}


	private void processStatement(Statement s, FileCacheStatement fileStatement) {
		String triple = s.getSubject().stringValue() + " " + s.getPredicate().stringValue() + " "
				+ s.getObject().stringValue();
		if (!comparator.compare(triple)) {
			if (uniq > limit) {
				fileStatement.writeStatement(s);
				triplesInFile++;
				if (triplesInFile % msgInterval == 0) {
					logger.info(triplesInFile + " statements saved into file!");
				}
				keepProcessing = true;
			} else {
				comparator.add(triple);
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
	
	private void deleteTmpFileIfExist() {
		new File(LODVaderProperties.TMP_FOLDER, fileName + 1).delete();
		new File(LODVaderProperties.TMP_FOLDER, fileName + 2).delete();
	}

}
