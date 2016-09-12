package main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.loader.StartLODVader;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CLODFileParser;
import lodVader.processor.LodVaderProcessor;
import lodVader.streaming.StreamAndSaveBF;
import services.mongodb.dataset.DatasetServices;

/**
 * 
 */

/**
 * @author Ciro Baron Neto
 * 
 * Sep 11, 2016
 */
public class Main {
	
	
	final static Logger logger = LoggerFactory.getLogger(Main.class);
	
	
	public static void main(String[] args) {
		
		
		
		new StartLODVader();
		 
//		// parse CLOD file
		DescriptionFileParserLoader.load(new CLODFileParser("http://localhost/urls", "nt"));
		
		
		// get all datasets
		new DatasetServices().getDatasets(false).forEach((dataset) -> {
			
			try {
				new LodVaderProcessor().datasetProcessor(dataset, new StreamAndSaveBF());
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
		});

		
	}

}
