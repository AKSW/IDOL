package lodVader.spring.REST;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lodVader.loader.StartLODVader;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CLODFileParser;
import lodVader.processor.LodVaderProcessor;
import lodVader.streaming.StreamAndSaveBF;
import services.mongodb.dataset.DatasetServices;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
//		if (LODVaderProperties.EVALUATE_LINKS) {
//			LinksCLOD l = new LinksCLOD();
////			l.checkCohesion();
////			l.checkCohesion(); 
////			LOV2 lov = new LOV2();
////			try { 
////				lov.loadLOVVocabularies();
////			} catch (Exception e) {
////				// TODO Auto-generated catch block
////				e.printStackTrace();
////			}
//			
//			TestLinks t = new TestLinks();
//			
//			
//		} 
//		
//		else {
			SpringApplication.run(Application.class, args);
			StartLODVader s = new StartLODVader(); 
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
////		}
	}
}
