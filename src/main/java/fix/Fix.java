/**
 * 
 */
package fix;

import lodVader.parsers.descriptionFileParser.Impl.CKANRepositoriesParser;
import lodVader.parsers.descriptionFileParser.Impl.CLODParser;
import lodVader.parsers.descriptionFileParser.Impl.DataIDParser;
import lodVader.parsers.descriptionFileParser.Impl.LODCloudParser;
import lodVader.parsers.descriptionFileParser.Impl.LOVParser;
import lodVader.parsers.descriptionFileParser.Impl.LinghubParser;
import lodVader.parsers.descriptionFileParser.Impl.RE3RepositoriesParser;
import lodVader.services.mongodb.MetadataParserServices;

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