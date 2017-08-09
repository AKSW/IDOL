package org.aksw.idol.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DecimalFormat;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.aksw.idol.exceptions.LODVaderFormatNotAcceptedException;
import org.aksw.idol.exceptions.LODVaderLODGeneralException;
import org.aksw.idol.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.exceptions.LODVaderSPARQLGraphNotFound;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.tupleManager.PipelineProcessor;
import org.aksw.idol.utils.FormatsUtils;
import org.aksw.idol.utils.SPARQLUtils;
import org.aksw.idol.utils.URLUtils;
import org.anarres.parallelgzip.ParallelGZIPInputStream;
import org.aksw.idol.utils.FormatsUtils.COMPRESSION_FORMATS;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.jsonld.JSONLDParser;
import org.openrdf.rio.n3.N3ParserFactory;
import org.openrdf.rio.nquads.NQuadsParserFactory;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.tql.TQL;
import eu.fbk.rdfpro.tql.TQLParserFactory;

public class LODVStreamInternetImpl implements IDOLStreamInterface {

	final static Logger logger = LoggerFactory.getLogger(LODVStreamInternetImpl.class);

	// HTTP header fields
	public String httpDisposition = null;
	public String httpContentType = null;
	public double httpContentLength;
	public String httpLastModified = "0";

	int timeout = 5000;

	private int redirection = 0;

	DistributionDB distribution;

	private PipelineProcessor pipelineProcessor;

	/**
	 * Constructor for Class LODVaderCoreStream
	 */
	public LODVStreamInternetImpl() {
		PipelineProcessor pipelineProcessor = new PipelineProcessor();
		this.pipelineProcessor = pipelineProcessor;
	}

	/**
	 * @return the tupleManager
	 */
	public PipelineProcessor getTupleManager() {
		return pipelineProcessor;
	}

	public PipelineProcessor getPipelineProcessor() {
		return this.pipelineProcessor;
	}

	protected void getMetadataFromHTTPHeaders(HttpURLConnection httpConn) {

		httpDisposition = httpConn.getHeaderField("Content-Disposition");
		httpContentType = httpConn.getContentType();
		httpContentLength = httpConn.getContentLength();

		if (httpConn.getLastModified() > 0)
			httpLastModified = String.valueOf(httpConn.getLastModified());

		printHeaders();

	}

	@Override
	public void startParsing(DistributionDB distributionMongoDBObj) throws Exception {
		this.distribution = distributionMongoDBObj;
		checkFormatTypeAndStartParsingAndStreaming(distributionMongoDBObj.getDownloadUrl(),
				distributionMongoDBObj.getFormat());
	}

	/**
	 * Analyze if the stream is from a sparql endpoint or a dumpfile
	 * 
	 * @param downloadUrl
	 * @param rdfFormat
	 * @throws IOException
	 * @throws LODVaderLODGeneralException
	 * @throws RDFParseException
	 * @throws RDFHandlerException
	 * @throws LODVaderSPARQLGraphNotFound
	 */
	public void checkFormatTypeAndStartParsingAndStreaming(String downloadUrl, String rdfFormat) throws IOException,
			LODVaderLODGeneralException, RDFParseException, RDFHandlerException, LODVaderSPARQLGraphNotFound {

		/**
		 * Check whether is a sparql endpoint.
		 */
		if (rdfFormat.equals(FormatsUtils.DEFAULT_SPARQL))
			prepareToSparqlEndpoint();

		/**
		 * If it is not, just stream!
		 */
		else
			streamAndParse(downloadUrl, rdfFormat);

	}

	/**
	 * Method used to start the sstreaming process
	 * 
	 * @param downloadUrl
	 * @param rdfFormat
	 * @throws IOException
	 * @throws RDFParseException
	 * @throws RDFHandlerException
	 */
	public void streamAndParse(String downloadUrl, String rdfFormat)
			throws IOException, RDFParseException, RDFHandlerException {

		InputStream inputStream = null;

		COMPRESSION_FORMATS compressionFormat;
		// TODO This is a last minute change. Please, change the laundromat
		// parser to detect the compression format.
		if (downloadUrl.contains("lodlaundromat"))
			compressionFormat = COMPRESSION_FORMATS.GZ;
		else
			compressionFormat = new FormatsUtils().getCompressionFormat(downloadUrl);

		try {

			inputStream = openConnection(downloadUrl, rdfFormat).getInputStream();
			inputStream = loadCompressors(new BufferedInputStream(inputStream), compressionFormat);
			if (rdfFormat.equals(""))
				rdfFormat = FormatsUtils.getEquivalentFormat(downloadUrl);

			startStream(inputStream, compressionFormat, rdfFormat);
		} catch (RDFParseException | RDFHandlerException e) {
			// Maybe we did not the the correct serialization format;
			// Try with TTL

			try {
				logger.info("Maybe we've got the wrong format.. trying TTL");

				inputStream = openConnection(downloadUrl, rdfFormat).getInputStream();
				inputStream = loadCompressors(new BufferedInputStream(inputStream), compressionFormat);
				rdfFormat = FormatsUtils.getEquivalentFormat("ttl");

				startStream(inputStream, compressionFormat, rdfFormat);

			} catch (RDFParseException | RDFHandlerException e2) {

				try {
					logger.info("Maybe we've got the wrong format.. trying RDF");

					// try with RDF
					inputStream = openConnection(downloadUrl, rdfFormat).getInputStream();
					inputStream = loadCompressors(new BufferedInputStream(inputStream), compressionFormat);
					rdfFormat = FormatsUtils.getEquivalentFormat("rdf");

					startStream(inputStream, compressionFormat, rdfFormat);

				} catch (RDFParseException | RDFHandlerException e3) {

					logger.info("Maybe we've got the wrong format.. trying NT");

					// try with NT
					inputStream = openConnection(downloadUrl, rdfFormat).getInputStream();
					inputStream = loadCompressors(new BufferedInputStream(inputStream), compressionFormat);
					rdfFormat = FormatsUtils.getEquivalentFormat("nt");

					startStream(inputStream, compressionFormat, rdfFormat);
				}
			}

		} finally {
			try {
				inputStream.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

	}

	public void prepareToSparqlEndpoint()
			throws LODVaderSPARQLGraphNotFound, IOException, RDFParseException, RDFHandlerException {

		/**
		 * Check whether there is a graph to be queried
		 */
		if (distribution.getSparqlGraph() == null) {
			throw new LODVaderSPARQLGraphNotFound("We couldn't find a graph within the endpoint.");
		}

		// set timeout to one min
		timeout = 60_000;

		/**
		 * Discover the number of triples within the triplestore (for pagination
		 * purposes)
		 */

		SPARQLUtils sparqlUtils = new SPARQLUtils();

		String srTriplesQuery = sparqlUtils.makeCountQuery(distribution.getSparqlEndpoint(),
				distribution.getSparqlGraph());

		long nrTriples = sparqlUtils.parseCounterResult(
				openConnection(sparqlUtils.encodeSparqlQuery(srTriplesQuery), "ttl").getInputStream());

		// if graph has no triples, get out of here
		if (nrTriples == 0) {
			throw new LODVaderSPARQLGraphNotFound("The graph " + distribution.getSparqlGraph() + " has 0 triple. ");
		}

		logger.debug("SPARQL Graph found! ");
		logger.debug("Endpoint: " + distribution.getSparqlEndpoint());
		logger.debug("Graph: " + distribution.getSparqlGraph());
		logger.info("Triples: " + nrTriples);

		distribution.setSparqlCount((int) nrTriples);
		try {
			distribution.update();
		} catch (LODVaderMissingPropertiesException e) {
			e.printStackTrace();
		}

		/**
		 * Start pagination
		 */
		int offset_step = 1_000_000;
		int current_offset = 0;
		while (current_offset < distribution.getSparqlCount()) {
			String newUrl = sparqlUtils.createSparqlPaginationRequest(distribution.getSparqlEndpoint(),
					distribution.getSparqlGraph(), current_offset + offset_step, current_offset);
			current_offset = current_offset + offset_step;
			logger.info("Streaming paginated sparql request: " + newUrl);
			streamAndParse(sparqlUtils.encodeSparqlQuery(newUrl), "ttl");
			if (pipelineProcessor.getTriplesProcessed() < offset_step)
				break;
		}

	}

	public RDFParser getSuitableParser(String rdfFormat) throws IOException, LODVaderFormatNotAcceptedException {
		RDFParser rdfParser = null;

		TQL.register();

		// checking whether to use turtle parser
		if (rdfFormat.equals(FormatsUtils.DEFAULT_TURTLE)) {
			rdfParser = new TurtleParser();
			logger.info("==== Turtle Parser loaded ====");
		}

		// checking ntriples format
		else if (rdfFormat.equals(FormatsUtils.DEFAULT_NTRIPLES)) {
			// rdfParser = new NTriplesParser();
			rdfParser = new NTriplesParser();
			logger.info("==== NTriples Parser loaded ====");
		}

		// checking rdf/xml format
		else if (rdfFormat.equals(FormatsUtils.DEFAULT_RDFXML)) {
			rdfParser = new RDFXMLParser();
			logger.info("==== RDF/XML Parser loaded ====");
		}

		// checking jsonld format
		else if (rdfFormat.equals(FormatsUtils.DEFAULT_JSONLD)) {
			rdfParser = new JSONLDParser();
			logger.info("==== JSON-LD Parser loaded ====");
		}

		// checking n3 format
		else if (rdfFormat.equals(FormatsUtils.DEFAULT_N3)) {
			rdfParser = new N3ParserFactory().getParser();
			logger.info("==== N3Parser loaded ====");
		}

		// checking TQL format
		else if (rdfFormat.equals(FormatsUtils.DEFAULT_TQL)) {
			rdfParser = new TQLParserFactory().getParser();
			logger.info("==== TQLParser loaded ====");
		}

		// checking NQ format
		else if (rdfFormat.equals(FormatsUtils.DEFAULT_NQUADS)) {
			rdfParser = new NQuadsParserFactory().getParser();
			logger.info("==== NQuads loaded ====");
		}

		// if the format is not supported, throw an exception
		else {
			logger.error("RDF format not supported: " + rdfFormat);
			throw new LODVaderFormatNotAcceptedException("RDF format not supported: " + rdfFormat);
		}

		// set RDF handler
		rdfParser.setRDFHandler(pipelineProcessor);

		// set OpenRDF parset config
		ParserConfig config = new ParserConfig();
		config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
		config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
		config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
		config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
		config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
		rdfParser.setParserConfig(config);

		return rdfParser;
	}

	private void startStream(InputStream inputStream, COMPRESSION_FORMATS compressionFormat, String rdfFormat)
			throws IOException, RDFParseException, RDFHandlerException {

		// try {
		// check whether file is tar/zip type
		if (compressionFormat.equals(FormatsUtils.COMPRESSION_FORMATS.ZIP)) {
			InputStream data = new BufferedInputStream(inputStream);
			logger.debug("File extension is zip, creating ZipInputStream and checking compressed files...");

			ZipInputStream zip = new ZipInputStream(data);
			int nf = 0;
			ZipEntry entry = zip.getNextEntry();
			while (entry != null) {
				if (!entry.isDirectory()) {
					logger.info(++nf + " zip file(s) uncompressed.");
					logger.info("File name: " + entry.getName());

					rdfFormat = FormatsUtils.getEquivalentFormat(entry.getName());

					if (!rdfFormat.equals("")) {

						File f = new File(LODVaderProperties.TMP_FOLDER + "/" + distribution.getID());

						try {
							RDFParser rdfParser = getSuitableParser(rdfFormat);

							simpleDownload(LODVaderProperties.TMP_FOLDER + "/" + distribution.getID(), zip);
							try {
								rdfParser.parse(new FileInputStream(f), "");
							} catch (Exception e) {
								e.printStackTrace();
							}
						} catch (LODVaderFormatNotAcceptedException e1) {
							try{
							inputStream.close();
							}
							catch (Exception e) {
								e.printStackTrace();
							}
							e1.printStackTrace();
						}
						f.delete();
					} else {
						logger.info("File extension not supported: " + entry.getName());
					}

				}
				entry = zip.getNextEntry();
			}
		}

		else if (compressionFormat.equals(FormatsUtils.COMPRESSION_FORMATS.TAR)) {

			InputStream data = new BufferedInputStream(inputStream);
			logger.info("File extension is tar, creating TarArchiveInputStream and checking compressed files...");

			TarArchiveInputStream tar = new TarArchiveInputStream(data);
			int nf = 0;
			TarArchiveEntry entry = (TarArchiveEntry) tar.getNextEntry();
			while (entry != null) {
				if (entry.isFile() && !entry.isDirectory()) {

					logger.info(++nf + " tar file(s) uncompressed.");
					logger.info("File name: " + entry.getName());

					File f = new File(LODVaderProperties.TMP_FOLDER + "/" + distribution.getID());

					rdfFormat = FormatsUtils.getEquivalentFormat(entry.getName());
					if (!rdfFormat.equals("")) {
						COMPRESSION_FORMATS newCompressionFormat = new FormatsUtils()
								.getCompressionFormat(entry.getName());

						// check if file is not compressed

						if (!newCompressionFormat.equals(COMPRESSION_FORMATS.NO_COMPRESSION)) {
							simpleDownload(LODVaderProperties.TMP_FOLDER + "/" + distribution.getID(),
									loadCompressors(new BufferedInputStream(tar), newCompressionFormat));
						} else {
							simpleDownload(LODVaderProperties.TMP_FOLDER + "/" + distribution.getID(), tar);
						}

						try {
							RDFParser rdfParser = getSuitableParser(rdfFormat);
							rdfParser.parse(new FileInputStream(f), "");
						} catch (LODVaderFormatNotAcceptedException e) {
							e.printStackTrace();
						}
					}

				}
				entry = (TarArchiveEntry) tar.getNextEntry();
			}
		}

		else {
			try {
				RDFParser rdfParser = getSuitableParser(rdfFormat);
				rdfParser.parse(inputStream, "");

			} catch (LODVaderFormatNotAcceptedException e) {
				e.printStackTrace();
			}
		}

		// } catch (RDFHandlerException | IOException | RDFParseException e) {
		// e.printStackTrace();
		// }

	}

	/**
	 * Check compression format (bz2, gz and tgz) and add the appropriate
	 * compressor inputstream
	 * 
	 * @param stream
	 * @return
	 * @throws IOException
	 * @throws LODVaderLODGeneralException
	 */
	protected InputStream loadCompressors(BufferedInputStream stream, COMPRESSION_FORMATS compressionFormat)
			throws IOException {

		// allowing bzip2 format
		if (compressionFormat.equals(FormatsUtils.COMPRESSION_FORMATS.BZ2))
			return checkBZip2InputStream(stream);

		// allowing gzip format
		if (compressionFormat.equals(FormatsUtils.COMPRESSION_FORMATS.GZ)
				|| compressionFormat.equals(FormatsUtils.COMPRESSION_FORMATS.TGZ))
			return checkGZipInputStream(stream, compressionFormat);

		return stream;
	}

	public static String read(InputStream input) throws IOException {
		try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
			return buffer.lines().collect(Collectors.joining("\n"));
		}
	}

	/**
	 * Open an HTTP connection following links and setting HTTP headers
	 * 
	 * @param downloadUrl
	 *            the address of the connection
	 * @param rdfFormat
	 * @throws IOException
	 * @throws LODVaderLODGeneralException
	 * @return HttpURLConnection
	 */
	public HttpURLConnection openConnection(String downloadUrl, String rdfFormat) throws IOException {

		HttpURLConnection httpConn = (HttpURLConnection) new URL(downloadUrl).openConnection();

		// if file format is unknown, try to fetch TTL data
		if (rdfFormat != null)
			if (rdfFormat.equals("ttl")) {
				httpConn.setRequestProperty("Accept", "text/turtle");
			} else if (rdfFormat.equals("rdf") || rdfFormat.equals("")) {
				httpConn.setRequestProperty("Accept", "application/rdf+xml");
			}

		logger.info("Opening HTTP connection for URL: " + downloadUrl.toString());

		httpConn.setReadTimeout(timeout);
		httpConn.setConnectTimeout(timeout);
		int responseCode = httpConn.getResponseCode();

		logger.info("We received the following HTTP response code: " + responseCode);

		// check HTTP response code
		if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP || responseCode == HttpURLConnection.HTTP_MOVED_PERM
				|| responseCode == HttpURLConnection.HTTP_SEE_OTHER) {
			downloadUrl = httpConn.getHeaderField("Location");
			logger.info("Redirecting connection to URL: " + downloadUrl);
			redirection++;
			if (redirection == 10)
				throw new IOException("Too many redirections!");
			openConnection(downloadUrl, rdfFormat);
		}

		else if (responseCode != HttpURLConnection.HTTP_OK) {
			httpConn.disconnect();
			logger.info("We received the following HTTP response code: " + responseCode);
			throw new IOException("No file to download. Server replied HTTP code: " + responseCode);
		}

		logger.info("Successfuly connected with HTTP OK status.");

		return httpConn;

	}

	protected void printHeaders() {
		DecimalFormat df = new DecimalFormat("#.##");

		logger.debug("Content-Type = " + httpContentType);
		logger.debug("Last-Modified = " + httpLastModified);
		logger.debug("Content-Disposition = " + httpDisposition);
		logger.debug("Content-Length = " + df.format(httpContentLength / 1024 / 1024) + " MB");
	}

	protected BZip2CompressorInputStream checkBZip2InputStream(InputStream stream) throws IOException {
		logger.info("File extension is bz2, creating BZip2CompressorInputStream...");
		BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new BufferedInputStream(stream), true);
		logger.info("Done creating BZip2CompressorInputStream!");
		return inputStream;
	}

	public ParallelGZIPInputStream checkGZipInputStream(InputStream stream, COMPRESSION_FORMATS compressionFormat)
			throws IOException {
		logger.info("File extension is " + compressionFormat + ", creating GzipCompressorInputStream...");
		
		ParallelGZIPInputStream inputStream = new ParallelGZIPInputStream(new BufferedInputStream(stream));
//		GzipCompressorInputStream inputStream = new GzipCompressorInputStream(new BufferedInputStream(stream), true);

		if (compressionFormat.equals(FormatsUtils.COMPRESSION_FORMATS.TGZ))
			compressionFormat = FormatsUtils.COMPRESSION_FORMATS.TAR;
		logger.info("Done creating GzipCompressorInputStream!");
		return inputStream;
	}

	/**
	 * Stream a file.
	 * 
	 * @param file
	 *            the file name
	 * @param stream
	 *            the inputStream
	 */
	public void simpleDownload(String file, InputStream stream) {
		try {
			ReadableByteChannel rbc = Channels.newChannel(stream);
			FileOutputStream fos = new FileOutputStream(file);
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			
			fos.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
