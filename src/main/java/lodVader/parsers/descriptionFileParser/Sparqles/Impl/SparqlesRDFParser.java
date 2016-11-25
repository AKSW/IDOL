/**
 * 
 */
package lodVader.parsers.descriptionFileParser.Sparqles.Impl;

import java.io.IOException;
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
import lodVader.utils.ConnectionUtils;
import lodVader.utils.FormatsUtils;

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
		FormatsUtils futils = new FormatsUtils();
		List<String> r = new ArrayList<>();
		try {
			Model m = ModelFactory.createDefaultModel();
			m.read(stream, null, futils.getJenaFormat("ttl"));

			StmtIterator stmti = m.listStatements(null, RDFResourcesTags.resValue, (RDFNode) null);
			while (stmti.hasNext()) {
				r.add(stmti.next().getObject().toString());
			}
		} catch (org.apache.jena.riot.RiotException | org.apache.jena.atlas.AtlasException e) {
			ConnectionUtils conn = new ConnectionUtils();
			logger.error(e.getMessage());
			logger.info("Trying rdf format.");
			Model m = ModelFactory.createDefaultModel();
			try {
				m.read(conn.getStream(address, ConnectionUtils.RDF_HEADER), null, futils.getJenaFormat("rdf"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			StmtIterator stmti = m.listStatements(null, RDFResourcesTags.resValue, (RDFNode) null);
			while (stmti.hasNext()) {
				r.add(stmti.next().getObject().toString());
			}
			return null;
		}
		if (r.size() > 0)
			return r;
		return null;
	}

}
