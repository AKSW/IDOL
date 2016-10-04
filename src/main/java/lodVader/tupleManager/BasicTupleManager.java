package lodVader.tupleManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;

import lodVader.tupleManager.processors.BasicProcessorInterface;

public class BasicTupleManager extends RDFHandlerBase {

	// set of statistical data processors
	List<BasicProcessorInterface> processors = new ArrayList<BasicProcessorInterface>();

	/**
	 * Register a new processor
	 * 
	 * @param statisticalProcessor
	 */
	public void registerProcessor(BasicProcessorInterface processor) {
		processors.add(processor);
	}

	@Override
	public void handleStatement(Statement st) {

		// invoke all statistical processors
		if (processors.size() > 0)
			for (BasicProcessorInterface processor : processors) {
				processor.process(st);
			}
	}

}
