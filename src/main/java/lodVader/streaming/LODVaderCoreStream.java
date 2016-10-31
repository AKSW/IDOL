package lodVader.streaming;

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
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.tupleManager.PipelineProcessor;
import lodVader.utils.FormatsUtils;
import lodVader.utils.FormatsUtils.COMPRESSION_FORMATS;

public class LODVaderCoreStream {

	final static Logger logger = LoggerFactory.getLogger(LODVaderCoreStream.class);

	// HTTP header fields
	public String httpDisposition = null;
	public String httpContentType = null;
	public double httpContentLength;
	public String httpLastModified = "0";

	private int redirection = 0;

	DistributionDB distribution;

	// COMPRESSION_FORMATS compressionFormat;

	protected static final int BUFFER_SIZE = 1024 * 256;
	// public URL downloadUrl = null;

	// public InputStream inputStream = null;

	final byte[] buffer = new byte[BUFFER_SIZE];
	int n = 0;
	int aux = 0;

	// public String fileName = null;
	// public String extension = null;
	// public String RDFFormat = null;

	public String objectFilePath;

	public String hashFileName = null;
	public double contentLengthAfterDownloaded = 0;

	// HttpURLConnection httpConn = null;

	String accessURL = null;

	private PipelineProcessor pipelineProcessor;

	/**
	 * Constructor for Class LODVaderCoreStream
	 */
	public LODVaderCoreStream() {
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

	// public void startParsing(DistributionDB distributionMongoDBObj)
	// throws IOException, LODVaderLODGeneralException,
	// LODVaderFormatNotAcceptedException {
	// this.downloadUrl = new URL(distributionMongoDBObj.getDownloadUrl());
	// this.compressionFormat = new
	// FormatsUtils().getCompressionFormat(distributionMongoDBObj.getDownloadUrl());
	// this.RDFFormat = distributionMongoDBObj.getFormat();
	// this.distribution = distributionMongoDBObj;
	// logger.info("Let's try to read: "+this.downloadUrl .toString());
	//
	// InputStream stream = this.downloadUrl.openStream();
	//
	//// stream = checkCompression(new BufferedInputStream(stream));
	// discoverRDFFormat(distributionMongoDBObj.getDownloadUrl());
	//// openStream();
	// startStream();
	// }

	public void startParsing(DistributionDB distributionMongoDBObj) {
		this.distribution = distributionMongoDBObj;
		startParsing(distributionMongoDBObj.getDownloadUrl(), distributionMongoDBObj.getFormat());
	}

	public void startParsing(String downloadUrl, String rdfFormat) {
		COMPRESSION_FORMATS compressionFormat = new FormatsUtils().getCompressionFormat(downloadUrl);
		try {
			InputStream inputStream = openConnection(downloadUrl, rdfFormat).getInputStream();
			inputStream = loadCompressors(new BufferedInputStream(inputStream), compressionFormat);
			if (rdfFormat.equals(""))
				rdfFormat = FormatsUtils.getEquivalentFormat(downloadUrl);

			startStream(inputStream, compressionFormat, rdfFormat);

		} catch (IOException | LODVaderLODGeneralException e) {
			e.printStackTrace();
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
			throws IOException {

		try {
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
							System.out.println(rdfFormat);

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
								inputStream.close();
								e1.printStackTrace();
							}
							f.delete();
						}
						else{
							logger.info("File extension not supported: " + entry.getName());
						}

					}

					// if(zip.)
					entry = zip.getNextEntry();
				}

				// setExtension(FilenameUtils.getExtension(getFileName()));
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

		} catch (RDFHandlerException | IOException | RDFParseException e) {
			e.printStackTrace();
		}

		inputStream.close();
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
	public HttpURLConnection openConnection(String downloadUrl, String rdfFormat)
			throws IOException, LODVaderLODGeneralException {

		HttpURLConnection httpConn = (HttpURLConnection) new URL(downloadUrl).openConnection();

		// if file format is unknown, try to fetch TTL data
		if (rdfFormat != null)
			if (rdfFormat.equals("ttl") || rdfFormat.equals("")) {
				httpConn.setRequestProperty("Accept", "text/turtle");
			}

		httpConn.setReadTimeout(5000);
		httpConn.setConnectTimeout(5000);
		int responseCode = httpConn.getResponseCode();

		logger.info("Opening HTTP connection for URL: " + downloadUrl.toString());

		// check HTTP response code
		if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP || responseCode == HttpURLConnection.HTTP_MOVED_PERM
				|| responseCode == HttpURLConnection.HTTP_SEE_OTHER) {
			downloadUrl = httpConn.getHeaderField("Location");
			logger.info("We received the following HTTP response code: " + responseCode);
			logger.info("Redirecting connection to URL: " + downloadUrl);
			redirection++;
			if (redirection == 10)
				throw new IOException("Too many redirections!");
			openConnection(downloadUrl, rdfFormat);
		}

		else if (responseCode != HttpURLConnection.HTTP_OK) {
			httpConn.disconnect();
			logger.info("We received the following HTTP response code: " + responseCode);
			throw new LODVaderLODGeneralException("No file to download. Server replied HTTP code: " + responseCode);
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

	public GzipCompressorInputStream checkGZipInputStream(InputStream stream, COMPRESSION_FORMATS compressionFormat)
			throws IOException {
		logger.info("File extension is " + compressionFormat + ", creating GzipCompressorInputStream...");
		GzipCompressorInputStream inputStream = new GzipCompressorInputStream(new BufferedInputStream(stream), true);

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
