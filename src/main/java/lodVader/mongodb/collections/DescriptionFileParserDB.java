/**
 * 
 */
package lodVader.mongodb.collections;

import lodVader.mongodb.DBSuperClass;
import lodVader.parsers.interfaces.DescriptionFileParserInterface;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class DescriptionFileParserDB extends DBSuperClass {

	public final static String COLLECTION_NAME = "descriptionFileParser";

	public final static String PARSER_NAME = "parserName";

	public final static String REPOSITORY_ADDRESS = "repositoryAddress";

	public final static String LAST_TIME_USED = "lastTimeUpdated";

	/**
	 * Constructor for Class DescriptionFileParser
	 * 
	 * @param collectionName
	 */
	public DescriptionFileParserDB(DescriptionFileParserInterface parser) {
		super(COLLECTION_NAME);
		setParserName(parser.getParserName());		
		setRepositoryAddress(parser.getRepositoryAddress());
		find(true, DescriptionFileParserDB.REPOSITORY_ADDRESS, getRepositoryAddress());
	}

	public void setParserName(String parserName) {
		addField(PARSER_NAME, parserName);
	}

	public String getParserName() {
		return getField(PARSER_NAME).toString();
	}

	public void setRepositoryAddress(String repositoryAddress) {
		addField(REPOSITORY_ADDRESS, repositoryAddress);
	}

	public String getRepositoryAddress() {
		return getField(REPOSITORY_ADDRESS).toString();
	}

	public void setLastTimeUsed(String lastTimeUsed) {
		addField(LAST_TIME_USED, lastTimeUsed);
	}

	public String getLastTimeUsed() {
		try {
			return getField(LAST_TIME_USED).toString();
		} catch (NullPointerException e) {
			return null;
		}
	}

}
