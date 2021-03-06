/**
 * 
 */
package org.aksw.idol.services.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.aksw.idol.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.file.FileStatementCustom;
import org.aksw.idol.loader.LODVaderProperties;
import org.aksw.idol.mongodb.collections.DistributionDB;
import org.aksw.idol.mongodb.collections.MetadataParserDB;
import org.aksw.idol.mongodb.queries.GeneralQueriesHelper;
import org.aksw.idol.parsers.descriptionFileParser.MetadataParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class MetadataParserServices {
	
	final static Logger logger = LoggerFactory.getLogger(MetadataParserServices.class);

	/**
	 * Save a new parser into mongodb
	 * @param parser
	 */
	public void saveParser(MetadataParser parser) {
		MetadataParserDB m = new MetadataParserDB(parser);
		m.update(true, MetadataParserDB.PARSER_NAME, parser.getParserName());
	}

	
	/**
	 * Return a list of dump files from the parser
	 * @param parserName the parser name
	 * @return
	 */
	public List<FileStatementCustom> getFilesFromParser(String parserName) {
		List<FileStatementCustom> files = new ArrayList<>();
		ArrayList<DBObject> parsers = new GeneralQueriesHelper().getObjects(MetadataParserDB.COLLECTION_NAME,
				MetadataParserDB.PARSER_NAME, parserName);
		if (parsers.size() > 0) {
			ArrayList<DBObject> distributions = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
					DistributionDB.DATASOURCE, parserName);
			for (DBObject o : distributions) {
				DistributionDB dist = new DistributionDB(o);
				files.add(new FileStatementCustom(LODVaderProperties.BASE_PATH + "/raw_files/", "__RAW_" + dist.getID()));
			}
		}
		return files;
	}
	
	/**
	 * Return a list of dump files from the parser
	 * @param parserName the parser name
	 * @return
	 */
	public Collection<DistributionDB> getDistributionsFromParser(String parserName) {
		Collection<DistributionDB> distributionsReturn = new ArrayList<>();
		ArrayList<DBObject> parsers = new GeneralQueriesHelper().getObjects(MetadataParserDB.COLLECTION_NAME,
				MetadataParserDB.PARSER_NAME, parserName);
		if (parsers.size() > 0) {
			ArrayList<DBObject> distributions = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME,
					DistributionDB.DATASOURCE, parserName);
			for (DBObject o : distributions) {
				DistributionDB dist = new DistributionDB(o);
				distributionsReturn.add(dist);
			}
		}
		return distributionsReturn;
	}

	/**
	 * Update the total and the unique number of triples of a datasource
	 * @param parser the parser
	 * @param uniq the number of unique triples
	 * @param total the total number of triples
	 */
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
	
	/**
	 * Remove all distributions of a datasource
	 * @param parser
	 */
	public void removeDistributions(MetadataParser parser){
		ArrayList<DBObject> obs = new GeneralQueriesHelper().getObjects(DistributionDB.COLLECTION_NAME, DistributionDB.DATASOURCE, parser.getParserName());
		for(DBObject o : obs){
			DistributionDB dist = new DistributionDB(o);
			if (dist.getDatasources().size()>1){
				HashSet<String> h = new HashSet<>(dist.getDatasources());
				h.remove(parser.getParserName());
				dist.setDatasource(new ArrayList<>(h));
				try {
					dist.update();
				} catch (LODVaderMissingPropertiesException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else{
				DistributionServices ds = new DistributionServices();
				ds.removeDistribution(dist, true);
			}
		}
		
	}

}
