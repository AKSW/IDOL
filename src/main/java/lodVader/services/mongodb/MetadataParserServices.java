/**
 * 
 */
package lodVader.services.mongodb;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.MetadataParserDB;
import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.parsers.descriptionFileParser.MetadataParser;
import lodVader.utils.FileStatement;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class MetadataParserServices {

	public void saveParser(MetadataParser parser) {
		MetadataParserDB m = new MetadataParserDB(parser);
		m.update(true, MetadataParserDB.PARSER_NAME, parser.getParserName());

	}

	public List<FileStatement> getFilesFromParser(String parserName) {

		List<FileStatement> files = new ArrayList<>();
		ArrayList<DBObject> parsers = new GeneralQueriesHelper().getObjects(MetadataParserDB.COLLECTION_NAME,
				MetadataParserDB.PARSER_NAME, parserName);
		if (parsers.size() > 0) {
			ArrayList<DBObject> distributions = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
					DistributionDB.DATASOURCE, parserName);
			for (DBObject o : distributions) {
				DistributionDB dist = new DistributionDB(o);
				files.add(new FileStatement(LODVaderProperties.BASE_PATH + "/raw_files/", "__RAW_" + dist.getID()));
			}
		}
		return files;
	}

	public void updateTriples(MetadataParser parser, long uniq, long total) {
		ArrayList<DBObject> parsers = new GeneralQueriesHelper().getObjects(MetadataParserDB.COLLECTION_NAME,
				MetadataParserDB.PARSER_NAME, parser.getParserName());

		if (parsers.size() > 0) {
			MetadataParserDB m = new MetadataParserDB(parser);
			m.setUniqTriples(uniq);
			m.setTotalTriples(total);
			try {
				m.update();
			} catch (LODVaderMissingPropertiesException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
