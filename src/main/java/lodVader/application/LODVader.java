/**
 * 
 */
package lodVader.application;

import lodVader.application.fileparser.CKANRepositories;
import lodVader.loader.LODVaderConfigurator;
import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CLODFileParser;
import lodVader.parsers.descriptionFileParser.Impl.DataIDFileParser;
import lodVader.parsers.descriptionFileParser.Impl.LOVParser;
import lodVader.parsers.descriptionFileParser.Impl.LodCloudParser;

/**
 * @author Ciro Baron Neto
 * 
 * Main LODVader Application
 * 
 * Oct 1, 2016
 */
public class LODVader {

	
	public static void main(String[] args) {
		new LODVader().Manager();
	}
	
	/**
	 * Main method
	 */
	public void Manager(){
		
		// 
		LODVaderConfigurator s = new LODVaderConfigurator();
		s.configure();
		parseFiles();
		
//		DistributionDB distribution = new DistributionDB(854);
//		DistributionDB distribution = new DistributionDB();
//		distribution.setID("57f29a53b5c0f6664486dd86");
//		distribution.find();
//		SuperStream stream = new StreamAndSaveBF();
//		try {
//			stream.streamDistribution(distribution);
//		} catch (RDFHandlerException | RDFParseException | IOException | LODVaderLODGeneralException
//				| InterruptedException | LODVaderFormatNotAcceptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		
//		BasicTupleManager tupleManager = new BasicTupleManager();
//		
//		LodVaderStreamProcessor streamProcessor = new LodVaderStreamProcessor();
//		
//		BasicStatisticalDataProcessor basicStatisticalProcessor = new BasicStatisticalDataProcessor(distribution);
//		BloomFilterProcessor bfProcessor = new BloomFilterProcessor(distribution);
//		
//		tupleManager.registerProcessor(basicStatisticalProcessor);
//		tupleManager.registerProcessor(bfProcessor);
//		
//		
//		streamProcessor.setTupleManager(tupleManager);		
//		try {
//			streamProcessor.startParsing(distribution);
//		} catch (IOException | LODVaderLODGeneralException | LODVaderFormatNotAcceptedException e) {
//			e.printStackTrace();
//		}  
//		
//		basicStatisticalProcessor.saveStatisticalData();
//		bfProcessor.saveFilters();
//		
//		DatasetBFBucketDB bucket = new DatasetBFBucketDB();
//		bucket.saveCache(tupleManager., distributionMongoDBObj.getLODVaderID());
		
		

		
	}


	
	/**
	 * Parse description files such as DCAT, VoID, DataID, CKAN repositories, etc.
	 */
	public void parseFiles(){
		
		// load ckan repositories into lodvader
		CKANRepositories ckanParsers = new CKANRepositories();
		ckanParsers.loadAllRepositories();
		
		DescriptionFileParserLoader loader = new DescriptionFileParserLoader();
//		loader.load(new LodCloudParser());
//		loader.parse();

//		loader.load(new CLODFileParser("http://localhost/urls", "ttl"));
//		loader.parse();	
		
//		loader.load(new DataIDFileParser("http://downloads.dbpedia.org/2015-10/2015-10_dataid_catalog.ttl"));
//		loader.parse();	

//		loader.load(new LOVParser());
//		loader.parse();	
		

				
		
	}
	
}
