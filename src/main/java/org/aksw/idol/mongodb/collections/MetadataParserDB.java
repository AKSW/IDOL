/**
 * 
 */
package org.aksw.idol.mongodb.collections;

import org.aksw.idol.mongodb.DBSuperClass;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class MetadataParserDB extends DBSuperClass {

	public final static String COLLECTION_NAME = "descriptionFileParser";

	public final static String PARSER_NAME = "parserName";

	public final static String LAST_TIME_USED = "lastTimeUpdated";

	public final static String UNIQ_TRIPLES = "uniqTriples";

	public final static String TOTAL_TRIPLES = "totalTriples";

	/**
	 * Constructor for Class DescriptionFileParser
	 * 
	 * @param collectionName
	 */
	public MetadataParserDB(MetadataParser parser) {
		super(COLLECTION_NAME);
		setParserName(parser.getParserName());
		find(true, MetadataParserDB.PARSER_NAME, getParserName());
	}

	public void setParserName(String parserName) {
		addField(PARSER_NAME, parserName);
	}

	public long getUniqTriples() {
		return ((Number) getField(UNIQ_TRIPLES)).longValue();
	}

	public void setUniqTriples(long uniqTriples) {
		addField(UNIQ_TRIPLES, uniqTriples);
	}

	public long getTotalTriples() {
		return ((Number) getField(TOTAL_TRIPLES)).longValue();
	}

	public void setTotalTriples(long totalTriples) {
		addField(TOTAL_TRIPLES, totalTriples);
	}

	public String getParserName() {
		return getField(PARSER_NAME).toString();
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
