/**
 * 
 */
package fix;

import org.aksw.idol.parsers.descriptionFileParser.Impl.CKANRepositoriesParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.CLODParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.DataIDParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.LODCloudParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.LOVParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.LinghubParser;
import org.aksw.idol.parsers.descriptionFileParser.Impl.RE3RepositoriesParser;
import org.aksw.idol.services.mongodb.MetadataParserServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 14, 2016
 */
public class Fix {


	public void fix1() {
		MetadataParserServices service = new MetadataParserServices();
		service.saveParser(new LODCloudParser());
		service.saveParser(new LOVParser());
		service.saveParser(new DataIDParser(null));
		service.saveParser(new RE3RepositoriesParser(null, 0));
		service.saveParser(new CLODParser(null, null));
		service.saveParser(new LinghubParser(null));
		service.saveParser(new CKANRepositoriesParser());
	}


}