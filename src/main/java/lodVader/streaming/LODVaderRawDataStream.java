/**
 * 
 */
package lodVader.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.management.StandardEmitterMBean;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.application.LODVader;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.tupleManager.PipelineProcessor;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
public class LODVaderRawDataStream {

	final static Logger logger = LoggerFactory.getLogger(LODVaderRawDataStream.class);

	DistributionDB distribution = null;

	String path = null;

	PipelineProcessor pipelineProcessor = new PipelineProcessor();

	/**
	 * Constructor for Class LODVaderRawDataStream
	 */
	public LODVaderRawDataStream(String basePath) {
		this.path = basePath;
	}

	public PipelineProcessor getPipelineProcessor() {
		return pipelineProcessor;
	}

	public void startParsing(DistributionDB distribution) {
		this.distribution = distribution;

		try {
			logger.info("Loading: " + path + distribution.getID());
			BufferedReader br = new BufferedReader(new FileReader(new File(path + distribution.getID())));

			String line;
			while ((line = br.readLine()) != null) {
				String[] split = line.split(" ",3);
				if (split.length > 2)
					callProcessors(split[0], split[1], split[2]);
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Statement createStatement(String subject, String predicate, String object) {

		return new Statement() {

			@Override
			public Resource getSubject() {
				// TODO Auto-generated method stub
				return new Resource() {

					@Override
					public String stringValue() {
						return subject;
					}
				};
			}

			@Override
			public URI getPredicate() {
				return new URI() {

					@Override
					public String stringValue() {
						return predicate;
					}

					@Override
					public String getNamespace() {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public String getLocalName() {
						// TODO Auto-generated method stub
						return null;
					}
				};
			}

			@Override
			public Value getObject() {
				return new Value() {

					@Override
					public String stringValue() {
						return object;
					}
				};
			}

			@Override
			public Resource getContext() {
				// TODO Auto-generated method stub
				return null;
			}

		};

	}

	private void callProcessors(String subject, String predicate, String object) {
		Statement s = createStatement(subject, predicate, object);
		pipelineProcessor.handleStatement(s);
	}

}
