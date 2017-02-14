import java.io.IOException;
import java.io.InputStream;

import org.aksw.idol.utils.ConnectionUtils;
import org.aksw.idol.utils.SPARQLUtils;
import org.aksw.idol.utils.URLUtils;
import org.junit.Test;

/**
 * @author Ciro Baron Neto
 * 
 * Nov 23, 2016
 */
public class SparqlPaginationTest {
	
	
	@Test
	public void paginationTest() {
		
		ConnectionUtils conn = new ConnectionUtils();
		SPARQLUtils sparqlUtils = new SPARQLUtils();
		
		String sparqlEndpoint = "http://dbpedia.org/sparql";
		String s2 = "http://156.35.82.103/sparql?query=CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <http://purl.org/weso/moldeas/organization/> { ?s ?p ?o } }";
						// http://156.35.82.103/sparql?query=CONSTRUCT+%7B+%3Fs+%3Fp+%3Fo+%7D+WHERE+%7B+GRAPH+%3Chttp%3A%2F%2Fpurl.org%2Fweso%2Fwebindex%2Fv2012%2F%3E+%7B+%3Fs+%3Fp+%3Fo+%7D+%7D
		URLUtils u = new URLUtils();
		
		String countUrl  = sparqlUtils.makeCountQuery(sparqlEndpoint, "http://dbpedia.org");
		
		try {
			InputStream res = conn.getStream(sparqlUtils.encodeSparqlQuery(countUrl), ConnectionUtils.TTL_HEADER);
//			System.out.println(res);
			System.out.println(sparqlUtils.parseCounterResult(res)); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println(sparqlUtils.encodeSparqlQuery(countUrl));
		
	}
	
	

}
