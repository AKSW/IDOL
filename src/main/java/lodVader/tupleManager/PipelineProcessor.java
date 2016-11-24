package lodVader.tupleManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.application.LODVader;
import lodVader.tupleManager.processors.BasicProcessorInterface;

public class PipelineProcessor extends RDFHandlerBase {
	
	final static Logger logger = LoggerFactory.getLogger(PipelineProcessor.class);


	// set of statistical data processors
	List<BasicProcessorInterface> processors = new ArrayList<BasicProcessorInterface>();
	
	// number of triples processed
	int triplesProcessed = 0;	
	
	/**
	 * Register a new processor
	 * 
	 * @param statisticalProcessor
	 */
	public void registerProcessor(BasicProcessorInterface processor) {
		processors.add(processor);
	}
	
	/**
	 * @return the triplesProcessed
	 */
	public int getTriplesProcessed() {
		return triplesProcessed;
	}
	

	@Override
	public void handleStatement(Statement st) {
		
		triplesProcessed ++;
		
		// print number of processed triples
		if(triplesProcessed%1000000 == 0){
			logger.info(triplesProcessed + " triples processed.");
		}
		
		// invoke all statistical processors
		if (processors.size() > 0)
			for (BasicProcessorInterface processor : processors) {
				processor.process(st);
			}
	}

}
