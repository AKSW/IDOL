/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Sparqles.Impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import lodVader.ontology.RDFResourcesTags;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 22, 2016
 */
public class SparqlesRDFParser implements SparqlesDistributionParser {

	final static Logger logger = LoggerFactory.getLogger(SparqlesRDFParser.class);

	InputStream stream = null;

	/**
	 * Constructor for Class SparqlesRDFParser
	 */
	public SparqlesRDFParser(InputStream stream) {
		this.stream = stream;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lodVader.parsers.descriptionFileParser.Sparqles.Impl.
	 * SparqlesDistributionParser#parse(java.lang.String)
	 */
	@Override
	public List<String> parse(String address) {
		List<String> r = new ArrayList<>();
		try {
			Model m = ModelFactory.createDefaultModel();
			m.read(stream, null, "TTL");

			StmtIterator stmti = m.listStatements(null, RDFResourcesTags.resValue, (RDFNode) null);
			while (stmti.hasNext()) {
				r.add(stmti.next().getObject().toString());
			}
		} catch (org.apache.jena.riot.RiotException | org.apache.jena.atlas.AtlasException e) {
			logger.error(e.getMessage());
			return null;
		}
		if (r.size() > 0)
			return r;
		return null;
	}

}
