package lodVader.mongodb.collections;

import lodVader.mongodb.DBSuperClass;

public abstract class ResourceDB extends DBSuperClass {

	public static final String URI = "uri";

	public static final String LOD_VADER_ID = "lodVaderID";

	public static final String IS_VOCABULARY = "isVocabulary";

	public static final String TITLE = "title";

	public static final String LABEL = "label";

	public ResourceDB(String collectionName) {
		super(collectionName);
		setIsVocabulary(false);
	}

	public void setLodVaderID(int id) {
		addField(LOD_VADER_ID, id);
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

	public boolean getIsVocabulary() {
		return Boolean.getBoolean(getField(IS_VOCABULARY).toString());
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

	public Integer getLODVaderID() {
		if (getField(LOD_VADER_ID) != null)
			return ((Number) getField(LOD_VADER_ID)).intValue();
		else
			return null;
	}

	public String getUri() {
		return getField(URI).toString();
	}

	public void setUri(String uri) {
		addField(URI, uri);
	}
}
