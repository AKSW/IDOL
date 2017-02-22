package org.aksw.idol.core.entity;

import java.util.Collection;

import org.aksw.idol.utils.FormatsUtils;

/**
 * @author Ciro Baron Neto
 *
 * Feb 22, 2017
 */
public class Distribution {

	
	/**
	 * Constructor 
	 */
	public Distribution() {
		
	}
	
	/**
	 * The URL which the distribution is available
	 */
	String downloadURL;
	
	/**
	 * The distributions unique identifier
	 */
	String uri;
	
	
	/**
	 * The top parent dataset which contains this distribution
	 */
	Dataset topDataset;
	
	
	/**
	 * The last distribution info or error message
	 */
	String lastMessage;
	
	/**
	 * Distribution HTTP byte size
	 */
	long httpByteSize;
	
	/**
	 * Distribution HTTP format
	 */
	String httpFormat;
	
	/**
	 * Distribution HTTP last modification date
	 */
	String httpLastModified;
	
	/**
	 * Distribution compression format
	 */
	FormatsUtils.COMPRESSION_FORMATS compressionFormat;
	
	
	/**
	 * Distribution serialization format
	 */
	
	FormatsUtils.SERIALIZATION_FORMAT serializationFormat;
	
	
	/**
	 * Collection of parent datasets. Example: datasets or subsets which contain this distribution.
	 */
	Collection<Dataset> parentDatasets;
	

	/**
	 * Collection of repositories which contain this distribution.
	 */
	Collection<String> repositories;
	
	
	/**
	 * Collection of datasources which contain this distribution.
	 */
	Collection<String> datasources;
	
	/**
	 * The last time this distribution was streamed
	 */
	String lastTimeStreamed;
	
	/**
	 * Whether this distribution is a vocabulary/ontology or not
	 */
	Boolean isVocabulary;
	
	/**
	 * Number of blank nodes within the distribution
	 */
	long blankNodes;
	
	
	/**
	 * Distribution title
	 */
	String title;
	
	/**
	 * Distribution label
	 */
	String label;
	
	/**
	 * Number of triples within the distribution
	 */
	String numberOfTriples;
	
	/**
	 * Number of literals within the distribution
	 */
	String numberOfLiterals;
	
	
	/**
	 * Graph name case the distribution is within a SPARQL endpoint
	 */
	String SPARQLGraphName;
	
	/**
	 * SPARQL endpoint address
	 */
	String SPARQLEndPoint;
	
	
	
	/**
	 * 
	 * @author Ciro Baron Neto
	 *
	 * Multiple distribution status used to control the streaming process
	 * Feb 22, 2017
	 */
	public enum DistributionStatus {

		STREAMING,

		STREAMED,

		SEPARATING_SUBJECTS_AND_OBJECTS,

		WAITING_TO_STREAM,

		CREATING_BLOOM_FILTER,

		CREATING_LINKSETS,

		ERROR,

		DONE,

		CREATING_JACCARD_SIMILARITY,

		UPDATING_LINK_STRENGTH
	}



	/**
	 * @return the downloadURL
	 */
	public String getDownloadURL() {
		return downloadURL;
	}



	/**
	 * @param downloadURL the downloadURL to set
	 */
	public void setDownloadURL(String downloadURL) {
		this.downloadURL = downloadURL;
	}



	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}



	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}



	/**
	 * @return the topDataset
	 */
	public Dataset getTopDataset() {
		return topDataset;
	}



	/**
	 * @param topDataset the topDataset to set
	 */
	public void setTopDataset(Dataset topDataset) {
		this.topDataset = topDataset;
	}



	/**
	 * @return the lastMessage
	 */
	public String getLastMessage() {
		return lastMessage;
	}



	/**
	 * @param lastMessage the lastMessage to set
	 */
	public void setLastMessage(String lastMessage) {
		this.lastMessage = lastMessage;
	}



	/**
	 * @return the httpByteSize
	 */
	public long getHttpByteSize() {
		return httpByteSize;
	}



	/**
	 * @param httpByteSize the httpByteSize to set
	 */
	public void setHttpByteSize(long httpByteSize) {
		this.httpByteSize = httpByteSize;
	}



	/**
	 * @return the httpFormat
	 */
	public String getHttpFormat() {
		return httpFormat;
	}



	/**
	 * @param httpFormat the httpFormat to set
	 */
	public void setHttpFormat(String httpFormat) {
		this.httpFormat = httpFormat;
	}



	/**
	 * @return the httpLastModified
	 */
	public String getHttpLastModified() {
		return httpLastModified;
	}



	/**
	 * @param httpLastModified the httpLastModified to set
	 */
	public void setHttpLastModified(String httpLastModified) {
		this.httpLastModified = httpLastModified;
	}



	/**
	 * @return the compressionFormat
	 */
	public FormatsUtils.COMPRESSION_FORMATS getCompressionFormat() {
		return compressionFormat;
	}



	/**
	 * @param compressionFormat the compressionFormat to set
	 */
	public void setCompressionFormat(FormatsUtils.COMPRESSION_FORMATS compressionFormat) {
		this.compressionFormat = compressionFormat;
	}



	/**
	 * @return the serializationFormat
	 */
	public FormatsUtils.SERIALIZATION_FORMAT getSerializationFormat() {
		return serializationFormat;
	}



	/**
	 * @param serializationFormat the serializationFormat to set
	 */
	public void setSerializationFormat(FormatsUtils.SERIALIZATION_FORMAT serializationFormat) {
		this.serializationFormat = serializationFormat;
	}



	/**
	 * @return the parentDatasets
	 */
	public Collection<Dataset> getParentDatasets() {
		return parentDatasets;
	}



	/**
	 * @param parentDatasets the parentDatasets to set
	 */
	public void setParentDatasets(Collection<Dataset> parentDatasets) {
		this.parentDatasets = parentDatasets;
	}



	/**
	 * @return the repositories
	 */
	public Collection<String> getRepositories() {
		return repositories;
	}



	/**
	 * @param repositories the repositories to set
	 */
	public void setRepositories(Collection<String> repositories) {
		this.repositories = repositories;
	}



	/**
	 * @return the datasources
	 */
	public Collection<String> getDatasources() {
		return datasources;
	}



	/**
	 * @param datasources the datasources to set
	 */
	public void setDatasources(Collection<String> datasources) {
		this.datasources = datasources;
	}



	/**
	 * @return the lastTimeStreamed
	 */
	public String getLastTimeStreamed() {
		return lastTimeStreamed;
	}



	/**
	 * @param lastTimeStreamed the lastTimeStreamed to set
	 */
	public void setLastTimeStreamed(String lastTimeStreamed) {
		this.lastTimeStreamed = lastTimeStreamed;
	}



	/**
	 * @return the isVocabulary
	 */
	public Boolean getIsVocabulary() {
		return isVocabulary;
	}



	/**
	 * @param isVocabulary the isVocabulary to set
	 */
	public void setIsVocabulary(Boolean isVocabulary) {
		this.isVocabulary = isVocabulary;
	}



	/**
	 * @return the blankNodes
	 */
	public long getBlankNodes() {
		return blankNodes;
	}



	/**
	 * @param blankNodes the blankNodes to set
	 */
	public void setBlankNodes(long blankNodes) {
		this.blankNodes = blankNodes;
	}



	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}



	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}



	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}



	/**
	 * @param label the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
	}



	/**
	 * @return the numberOfTriples
	 */
	public String getNumberOfTriples() {
		return numberOfTriples;
	}



	/**
	 * @param numberOfTriples the numberOfTriples to set
	 */
	public void setNumberOfTriples(String numberOfTriples) {
		this.numberOfTriples = numberOfTriples;
	}



	/**
	 * @return the numberOfLiterals
	 */
	public String getNumberOfLiterals() {
		return numberOfLiterals;
	}



	/**
	 * @param numberOfLiterals the numberOfLiterals to set
	 */
	public void setNumberOfLiterals(String numberOfLiterals) {
		this.numberOfLiterals = numberOfLiterals;
	}



	/**
	 * @return the sPARQLGraphName
	 */
	public String getSPARQLGraphName() {
		return SPARQLGraphName;
	}



	/**
	 * @param sPARQLGraphName the sPARQLGraphName to set
	 */
	public void setSPARQLGraphName(String sPARQLGraphName) {
		SPARQLGraphName = sPARQLGraphName;
	}



	/**
	 * @return the sPARQLEndPoint
	 */
	public String getSPARQLEndPoint() {
		return SPARQLEndPoint;
	}



	/**
	 * @param sPARQLEndPoint the sPARQLEndPoint to set
	 */
	public void setSPARQLEndPoint(String sPARQLEndPoint) {
		SPARQLEndPoint = sPARQLEndPoint;
	}
	
	
	
	
}
