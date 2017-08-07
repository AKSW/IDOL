/**
 * 
 */
package org.aksw.idol.uniq;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
import org.aksw.idol.utils.ExternalSortLocal;
import org.aksw.idol.utils.ExternalSortLocalIDOL;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
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
public class DatasourcesUniqTriplesExternalSort {

	final static Logger logger = LoggerFactory.getLogger(DatasourcesUniqTriplesExternalSort.class);

	MetadataParser parser = null;

	String tmpFolder;

	String fileName = "IDOL_TMP_TRIPLES_RAW";

	String fileNameWithPath;

	String sortedFileNameWithPath;

	@Autowired
	public DatasourcesUniqTriplesExternalSort(Properties properties, FileCacheStatement fileStatement1,
			FileCacheStatement fileStatement2) {
		this.tmpFolder = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource().getTmpDir();
	}

	boolean keepProcessing = false;

	long uniq = 0;

	long total = 0;

	long totalUniq = 0;

	long totalTriples = 0;

	long triplesInFile = 0;

	int msgInterval = 1_000_000;

	DecimalFormat formatter = new DecimalFormat("#,###,###,###,###");

	BufferedOutputStream out;
	

	public void setup(MetadataParser parser) {
		fileNameWithPath = tmpFolder + "/" + fileName;
		sortedFileNameWithPath = fileNameWithPath + ".sorted";

		new File(fileNameWithPath).delete();
		new File(sortedFileNameWithPath).delete();
		this.parser = parser;
		try {
			out = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(new File(fileNameWithPath)),2048));
			countLoadingFromInternet();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	class CountProcessor implements BasicProcessorInterface {

		@Override
		public void process(Statement st) {
			processStatement(st);
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
			if (c < 150000)
				try {
					c++;
					stream.streamAndParse(d.getDownloadUrl(), d.getFormat());

				} catch (RDFParseException | RDFHandlerException | IOException e) {
					e.printStackTrace();
				}
			logger.info("Accumulated triples: " + formatter.format(totalTriples));
			logger.info("Processed datasets : " + formatter.format(c));
			logger.info("To be processed    : " + formatter.format((distributions.size() - c)));

		}

		// after finishing reading all triples, close the file
		out.close();

		// sort!

		ExternalSortLocalIDOL.sort(new File(fileNameWithPath), new File(sortedFileNameWithPath));

		
		
		totalUniq = uniqLines(sortedFileNameWithPath);

		updateCounter(totalUniq, totalTriples);

	}

	private void processStatement(Statement s) {
		StringBuilder triple = new StringBuilder();
		triple.append(s.getSubject().stringValue());
		triple.append(" ");
		triple.append(s.getPredicate().stringValue());
		triple.append(" ");
		triple.append(s.getObject().stringValue().replaceAll("(\r\n|\n)", ""));
		triple.append("\n");

		try {
			out.write(triple.toString().getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void updateCounter(long uniq, long total) {
		MetadataParserServices services = new MetadataParserServices();
		services.updateTriples(parser, uniq, total);
		logger.info("Counters updated!");
		logger.info("Total triples: " + formatter.format(total));
		logger.info("Unique triples: " + formatter.format(uniq));

	}

	public long countLines(String filename) throws IOException {
		InputStream is = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename)));
		try {
			byte[] c = new byte[1024];
			long count = 0;
			long readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
	}

	public long uniqLines(String filename) throws IOException {
		logger.info("Counting unique triples!");		

		BufferedReader bufferreader = new BufferedReader(new FileReader(filename));

		String line = bufferreader.readLine();
		String lastLine = "";

		long uniq = 0;

		while (line != null) {
			if (!line.equals(lastLine))
				uniq++;
			lastLine = line;
			line = bufferreader.readLine();
		}

		bufferreader.close();
		return uniq;
	}

}
