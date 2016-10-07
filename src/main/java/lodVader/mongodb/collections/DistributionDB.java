package lodVader.mongodb.collections;

import java.net.MalformedURLException;
import java.util.ArrayList;

import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.utils.URLUtils;

public class DistributionDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "Distribution";

	public DistributionDB() {
		super(COLLECTION_NAME);
		setKeys();
	}

	public DistributionDB(String downloadURL) {
		super(COLLECTION_NAME);
		try {
			setDownloadUrl(downloadURL);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		setKeys();
		find(true, DOWNLOAD_URL, downloadURL);
	}

	public DistributionDB(DBObject object) {
		super(COLLECTION_NAME);
		mongoDBObject = object;
		setKeys();
	}

	private void setKeys() {
		addMandatoryField(DOWNLOAD_URL);
		addMandatoryField(URI);
		addMandatoryField(STATUS);
	}

	// collection properties
	public static final String DOWNLOAD_URL = "downloadUrl";

	public static final String URI = "uri";

	public static final String TOP_DATASET = "topDataset";

	public static final String TOP_DATASET_TITLE = "topDatasetTitle";

	public static final String STATUS = "status";

	public static final String LAST_MSG = "lastMsg";

	public static final String HTTP_BYTE_SIZE = "httpByteSize";

	public static final String HTTP_FORMAT = "httpFormat";

	public static final String HTTP_LAST_MODIFIED = "httpLastModified";

	public static final String FORMAT = "format";

	public static final String ORIGINAL_FORMAT = "originalFormat";

	public static final String RESOURCE_URI = "resourceUri";

	public static final String DEFAULT_DATASETS = "defaultDatasets";

	public static final String LAST_TIME_STREAMED = "lastTimeStreamed";

	public static final String IS_VOCABULARY = "isVocabulary";

	public static final String TITLE = "title";

	public static final String LABEL = "label";

	public static final String NUMBER_OF_TRIPLES = "numberOfTriples";

	public static final String NUMBER_OF_LITERALS = "numberOfLiterals";
	
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

	public String getDownloadUrl() {
		return getField(DOWNLOAD_URL).toString();
	}

	public void setDownloadUrl(String downloadUrl) throws MalformedURLException {
		URLUtils utils = new URLUtils();
		// utils.validateURL(downloadUrl);
		addField(DOWNLOAD_URL, downloadUrl);
	}

	public String getHttpByteSize() {
		try {
			return getField(HTTP_BYTE_SIZE).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpByteSize(String httpByteSize) {
		addField(HTTP_BYTE_SIZE, httpByteSize);
	}

	public String getUri() {
		return getField(URI).toString();
	}

	public void setUri(String uri) {
		addField(URI, uri);
	}

	public void setNumberOfLiterals(int numberOfLiterals) {
		addField(NUMBER_OF_LITERALS, numberOfLiterals);
	}

	public void setNumberOfTriples(int numberOfTriples) {
		addField(NUMBER_OF_TRIPLES, numberOfTriples);
	}

	public int getNumberOfTriples() {
		return ((Number) getField(NUMBER_OF_TRIPLES)).intValue();
	}

	public int getNumberOfLiterals() {
		return ((Number) getField(NUMBER_OF_LITERALS)).intValue();
	}

	public String getTopDatasetID() {
		return getField(TOP_DATASET).toString();
	}

	public void setTopDataset(String topDataset) {
		addField(TOP_DATASET, topDataset);
	}

	public String getHttpFormat() {
		try {
			return getField(HTTP_FORMAT).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpFormat(String httpFormat) {
		addField(HTTP_FORMAT, httpFormat);
	}

	public String getHttpLastModified() {
		try {
			return getField(LAST_TIME_STREAMED).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpLastModified(String httpLastModified) {
		addField(HTTP_LAST_MODIFIED, httpLastModified);
	}

	public String getFormat() {
		return getField(FORMAT).toString();
	}

	public void setFormat(String format) {
		addField(FORMAT, format);
	}

	public String getOriginalFormat() {
		return getField(ORIGINAL_FORMAT).toString();
	}

	public void setOriginalFormat(String format) {
		addField(ORIGINAL_FORMAT, format);
	}

	public String getLastMsg() {

		if (getField(LAST_MSG) == null)
			return "";
		return getField(LAST_MSG).toString();
	}

	public void setLastMsg(String lastMsg) {
		addField(LAST_MSG, lastMsg);
	}

	public DistributionStatus getStatus() {
		try {
			getField(STATUS).toString();
		} catch (NullPointerException e) {
			return null;
		}
		return DistributionStatus.valueOf(getField(STATUS).toString());
	}

	public void setStatus(DistributionStatus status) {
		addField(STATUS, status.name());
	}

	public boolean getIsVocabulary() {
		return Boolean.parseBoolean(getField(IS_VOCABULARY).toString());
	}

	public String getResourceUri() {
		try {
			return getField(RESOURCE_URI).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setResourceUri(String resourceUri) {
		addField(RESOURCE_URI, resourceUri);
	}

	public String getLastTimeStreamed() {
		return getField(LAST_TIME_STREAMED).toString();
	}

	public void setLastTimeStreamed(String lastTimeStreamed) {
		addField(LAST_TIME_STREAMED, lastTimeStreamed);
	}

	public String getTopDatasetTitle() {
		return getField(TOP_DATASET_TITLE).toString();
	}

	public void setTopDatasetTitle(String topDatasetTitle) {
		addField(TOP_DATASET_TITLE, topDatasetTitle);
	}

	public void setDefaultDatasets(ArrayList<String> defaultDatasets) {
		addField(DEFAULT_DATASETS, defaultDatasets);
	}

	public void setIsVocabulary(boolean isVocabulary) {
		addField(IS_VOCABULARY, isVocabulary);
	}

	public String getTitle() {
		try {
			return getField(TITLE).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setTitle(String title) {
		addField(TITLE, title);
	}

	public void setLabel(String label) {
		addField(LABEL, label);
	}

	public String getLabel() {
		try {
			return getField(LABEL).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void addDefaultDatasets(String datasetID) {
		ArrayList<String> ids = (ArrayList<String>) getField(DEFAULT_DATASETS);
		if (ids != null) {
			if (!ids.contains(datasetID)) {
				ids.add(datasetID);
				addField(DEFAULT_DATASETS, ids);
			}
		} else {
			ids = new ArrayList<String>();
			ids.add(datasetID);
			addField(DEFAULT_DATASETS, ids);
		}
	}

	public ArrayList<Integer> getDefaultDatasets() {
		return (ArrayList<Integer>) getField(DEFAULT_DATASETS);
	}

	public ArrayList<DatasetDB> getDefaultDatasetsAsResources() {
		return new DatasetQueries().getDatasets((ArrayList<Integer>) getField(DEFAULT_DATASETS));
	}

}
