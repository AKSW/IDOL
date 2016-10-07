package lodVader.streaming;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DecimalFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FilenameUtils;
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
import lodVader.mongodb.collections.DistributionDB;
import lodVader.tupleManager.PipelineProcessor;
import lodVader.utils.FileUtils;
import lodVader.utils.FormatsUtils;

public class LodVaderCoreStream {

	final static Logger logger = LoggerFactory.getLogger(LodVaderCoreStream.class);

	// HTTP header fields
	public String httpDisposition = null;
	public String httpContentType = null;
	public double httpContentLength;
	public String httpLastModified = "0";

	protected static final int BUFFER_SIZE = 1024 * 256;
	public URL downloadUrl = null;

	public InputStream inputStream = null;

	final byte[] buffer = new byte[BUFFER_SIZE];
	int n = 0;
	int aux = 0;

	public String fileName = null;
	public String extension = null;
	public String RDFFormat = null;

	public String objectFilePath;

	public String hashFileName = null;
	public double contentLengthAfterDownloaded = 0;

	HttpURLConnection httpConn = null;

	String accessURL = null;

	private PipelineProcessor tupleManager;

	/**
	 * @return the tupleManager
	 */
	public PipelineProcessor getTupleManager() {
		return tupleManager;
	}

	/**
	 * @param tupleManager
	 *            Set the tupleManager value.
	 */
	public void registerPipelineProcessor(PipelineProcessor tupleManager) {
		this.tupleManager = tupleManager;
	}

	protected void getMetadataFromHTTPHeaders(HttpURLConnection httpConn) {

		httpDisposition = httpConn.getHeaderField("Content-Disposition");
		httpContentType = httpConn.getContentType();
		httpContentLength = httpConn.getContentLength();
		try {
			setFileName(httpConn.getHeaderField("Content-Disposition").split("filename=")[1]);
		} catch (Exception e) {

		}
		if (httpConn.getLastModified() > 0)
			httpLastModified = String.valueOf(httpConn.getLastModified());

		printHeaders();

	}

	public void startParsing(DistributionDB distributionMongoDBObj)
			throws IOException, LODVaderLODGeneralException, LODVaderFormatNotAcceptedException {
		this.downloadUrl = new URL(distributionMongoDBObj.getDownloadUrl());
		this.RDFFormat = distributionMongoDBObj.getFormat();
		openStream();
		setParser();
	}

	private void setParser() throws IOException, LODVaderFormatNotAcceptedException {

		// instance of rdf parser
		RDFParser rdfParser = null;
	
		
		TQL.register();
		
		// checking whether to use turtle parser
		if (RDFFormat.equals(FormatsUtils.DEFAULT_TURTLE)) {
			rdfParser = new TurtleParser();
			logger.info("==== Turtle Parser loaded ====");
		}

		// checking ntriples format
		else if (RDFFormat.equals(FormatsUtils.DEFAULT_NTRIPLES)) {
			rdfParser = new NTriplesParser();
			 rdfParser = new NTriplesParser();
			logger.info("==== NTriples Parser loaded ====");
		}

		// checking rdf/xml format
		else if (RDFFormat.equals(FormatsUtils.DEFAULT_RDFXML)) {
			rdfParser = new RDFXMLParser();
			logger.info("==== RDF/XML Parser loaded ====");
		}

		// checking jsonld format
		else if (RDFFormat.equals(FormatsUtils.DEFAULT_JSONLD)) {
			rdfParser = new JSONLDParser();
			logger.info("==== JSON-LD Parser loaded ====");
		}

		// checking n3 format
		else if (RDFFormat.equals(FormatsUtils.DEFAULT_N3)) {
			rdfParser = new N3ParserFactory().getParser();
			logger.info("==== N3Parser loaded ====");
		}
		
		// checking TQL format
		else if (RDFFormat.equals(FormatsUtils.DEFAULT_TQL)) {
			rdfParser = new TQLParserFactory().getParser();
			logger.info("==== TQLParser loaded ====");
		}
		
		// checking NQ format
		else if (RDFFormat.equals(FormatsUtils.DEFAULT_NQUADS)) {
			rdfParser = new NQuadsParserFactory().getParser();
			logger.info("==== NQuads loaded ====");
		}

		// if the format is not supported, throw an exception
		else {
			httpConn.disconnect();
			inputStream.close();
			logger.error("RDF format not supported: " + RDFFormat);
			throw new LODVaderFormatNotAcceptedException("RDF format not supported: " + RDFFormat);
		}

		try {

			// set RDF handler
			rdfParser.setRDFHandler(tupleManager);

			// set OpenRDF parset config
			ParserConfig config = new ParserConfig();
			config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
			config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
			config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
			config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
			config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
			rdfParser.setParserConfig(config);

			// check whether file is tar/zip type
			if (getExtension().equals("zip")) {
				InputStream data = new BufferedInputStream(inputStream);
				logger.debug("File extension is zip, creating ZipInputStream and checking compressed files...");

				ZipInputStream zip = new ZipInputStream(data);
				int nf = 0;
				ZipEntry entry = zip.getNextEntry();
				while (entry != null) {
					if (!entry.isDirectory()) {
						logger.debug(++nf + " zip file uncompressed.");
						logger.debug("File name: " + entry.getName());

						rdfParser.parse(zip, downloadUrl.toString());

					}

					entry = zip.getNextEntry();
				}

				setExtension(FilenameUtils.getExtension(getFileName()));
			}

			else if (getExtension().equals("tar")) {
				InputStream data = new BufferedInputStream(inputStream);
				logger.debug("File extension is tar, creating TarArchiveInputStream and checking compressed files...");

				TarArchiveInputStream tar = new TarArchiveInputStream(data);
				int nf = 0;
				TarArchiveEntry entry = (TarArchiveEntry) tar.getNextEntry();
				while (entry != null) {
					if (entry.isFile() && !entry.isDirectory()) {
						logger.debug(++nf + " tar file uncompressed.");
						logger.debug("File name: " + entry.getName());

						byte[] content = new byte[(int) entry.getSize()];

						tar.read(content, 0, (int) entry.getSize());

						rdfParser.parse(tar, downloadUrl.toString());
					}
					entry = (TarArchiveEntry) tar.getNextEntry();
				}
				setExtension(FilenameUtils.getExtension(getFileName()));
			}

			else {
				rdfParser.parse(inputStream, downloadUrl.toString());
			}

		} catch (RDFHandlerException | IOException | RDFParseException e) {
			e.printStackTrace();
		}

		httpConn.disconnect();
		inputStream.close();
	}

	protected void openStream() throws IOException, LODVaderLODGeneralException {
		openConnection();

		// opens input stream from HTTP connection
		inputStream = new BufferedInputStream(httpConn.getInputStream());

		logger.debug("InputStream from http connection opened");

		// get some data from headers
		getMetadataFromHTTPHeaders(httpConn);

		// allowing bzip2 format
		checkBZip2InputStream();

		// allowing gzip format
		checkGZipInputStream();

		// check format and extension
		if (RDFFormat == null || RDFFormat.equals("")) {
			DistributionDB dist = new DistributionDB();
			dist.find(true, DistributionDB.DOWNLOAD_URL, downloadUrl.toString());
			if (dist.getFormat() == null || dist.getFormat() == "" || dist.getFormat().equals(""))
				RDFFormat = getExtension();
			else
				RDFFormat = dist.getFormat();
		}
	}

	public void openConnection() throws IOException, LODVaderLODGeneralException {
		httpConn = (HttpURLConnection) downloadUrl.openConnection();

		httpConn.setReadTimeout(5000);
		httpConn.setConnectTimeout(5000);
		int responseCode = httpConn.getResponseCode();

		logger.info("Open HTTP connection for URL: " + downloadUrl.toString());

		// check HTTP response code
		if (responseCode != HttpURLConnection.HTTP_OK) {
			httpConn.disconnect();
			throw new LODVaderLODGeneralException("No file to download. Server replied HTTP code: " + responseCode);
		}

		logger.info("Successfuly connected with HTTP OK status.");

	}

	protected void printHeaders() {
		DecimalFormat df = new DecimalFormat("#.##");

		logger.debug("Content-Type = " + httpContentType);
		logger.debug("Last-Modified = " + httpLastModified);
		logger.debug("Content-Disposition = " + httpDisposition);
		logger.debug("Content-Length = " + df.format(httpContentLength / 1024 / 1024) + " MB");
		logger.debug("fileName = " + fileName);
	}

	protected void checkBZip2InputStream() throws IOException {

		// check whether file is bz2 type
		if (getExtension().equals("bz2")) {
			logger.info("File extension is bz2, creating BZip2CompressorInputStream...");
			httpConn = (HttpURLConnection) downloadUrl.openConnection();
			inputStream = new BZip2CompressorInputStream(new BufferedInputStream(httpConn.getInputStream()), true);
			setFileName(getFileName().replace(".bz2", ""));
			setExtension(null);

			logger.info("Done creating BZip2CompressorInputStream! New file name is " + getFileName());
		}
	}

	public void checkGZipInputStream() throws IOException {

		// check whether file is gz type
		if (getExtension().equals("gz") || getExtension().equals("tgz")) {
			logger.info("File extension is " + getExtension() + ", creating GzipCompressorInputStream...");
			logger.debug(new FileUtils().getFileName(downloadUrl.toString(), httpDisposition));
			httpConn = (HttpURLConnection) downloadUrl.openConnection();
			inputStream = new GzipCompressorInputStream(new BufferedInputStream(httpConn.getInputStream()), true);

			setFileName(getFileName().replace(".gz", ""));
			setFileName(getFileName().replace(".tgz", ".tar"));
			if (getFileName().contains(".tar"))
				setExtension("tar");
			if (getExtension().equals("tgz"))
				setExtension("tar");
			else
				setExtension(null);

			logger.info("Done creating GzipCompressorInputStream! New file name is " + getFileName() + ", extension: "
					+ getExtension());
		}

	}

	/**
	 * Get the file name of the current file being streamed
	 * 
	 * @return file name
	 */
	public String getFileName() {
		if (fileName == null) {
			// extracts file name from header field
			fileName = new FileUtils().getFileName(downloadUrl.toString(), httpDisposition);
			logger.debug("Found file name: " + fileName);
		}
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getExtension() {
		if (extension == null) {
			logger.info("Setting file extension.");
			extension = FilenameUtils.getExtension(getFileName());
			logger.info("File extension is: " + extension);
		}
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public URL getUrl() {
		return downloadUrl;
	}

	public void setUrl(URL url) {
		this.downloadUrl = url;
	}

	// public abstract void streamDistribution(DistributionDB distribution)
	// throws IOException, LODVaderLODGeneralException, InterruptedException,
	// RDFHandlerException,
	// RDFParseException, LODVaderFormatNotAcceptedException;

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
