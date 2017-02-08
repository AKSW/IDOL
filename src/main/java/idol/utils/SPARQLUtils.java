/**
 * 
 */
package idol.utils;

import java.io.InputStream;
import java.net.URLEncoder;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import idol.ontology.RDFResourcesTags;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 23, 2016
 */
public class SPARQLUtils {

	/**
	 * Encode the URL as a valid HTTP url, i.e. convert special characters.
	 * 
	 * @param url
	 * @return the valid HTTP url
	 */
	public String encodeSparqlQuery(String query) {
		String baseUrl = query.split("query=")[0];
		String toEncode = query.split("query=")[1];

		return baseUrl + "query=" + URLEncoder.encode(toEncode);
	}

	/**
	 * Creates a triplecount query.
	 * @param uri
	 * @param graph
	 * @return
	 */
	public String makeCountQuery(String uri, String graph) {

		uri = refineSparqlEndpointLink(uri);

		return uri + "SELECT (COUNT(*) as ?count) FROM <" + graph + "> WHERE { ?s ?p ?o . }";
	}
	
	private String refineSparqlEndpointLink(String uri){
		if (uri.endsWith("/")) {
			uri = uri.replace(uri.substring(uri.length() - 1), "");
		}

		if (!uri.endsWith("?query=")) {
			uri = uri + "?query=";
		}
		return uri;
	}

	
	/**
	 * Parses the result of a count query.
	 * @param counterResult
	 * @return the number of triples
	 */
	public long parseCounterResult(InputStream counterResult) {
		Model m = ModelFactory.createDefaultModel();
		m.read(counterResult, null, "TTL");


		StmtIterator it = m.listStatements(null, RDFResourcesTags.resValue, (RDFNode) null);
		while (it.hasNext()) {
			return it.next().getObject().asLiteral().getInt();
		}
		return 0;

	}

	public String getGraphQuery() {
		return "select distinct ?g where { graph ?g {?s ?p ?o} }";
	}

	/**
	 * Add SPARQL pagination into string.
	 * 
	 * @param uri
	 *            the URI
	 * @param offset
	 *            the offset value
	 * @param limit
	 *            the limit value
	 * @return the URI with " ORDER BY (?s) OFFSET offset LIMIT limit"
	 */
	public String createSparqlPaginationRequest(String sparqlEndpoint, String graph, int limit, int offset) {
		sparqlEndpoint = refineSparqlEndpointLink(sparqlEndpoint);
		
		String queryPart = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <" + graph + "> { ?s ?p ?o } }";

		return sparqlEndpoint + queryPart +  " LIMIT " + limit + " OFFSET " + offset;
	}

}
