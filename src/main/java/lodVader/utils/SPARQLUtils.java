/**
 * 
 */
package lodVader.utils;

import java.io.InputStream;
import java.net.URLEncoder;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import lodVader.ontology.RDFResourcesTags;

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

	public String makeCountQuery(String uri, String graph) {

		if (uri.endsWith("/")) {
			uri = uri.replace(uri.substring(uri.length() - 1), "");
		}

		if (!uri.endsWith("?query=")) {
			uri = uri + "?query=";
		}

		return uri + "SELECT (COUNT(*) as ?count) FROM <" + graph + "> WHERE { ?s ?p ?o . }";
	}

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
	public String addSparqlPagination(String uri, int offset, int limit) {
		return uri + " LIMIT " + limit + " OFFSET " + offset;
		// return uri + " ORDER BY (?s) "+ " LIMIT "+limit + " OFFSET " + offset
		// ;
	}

}