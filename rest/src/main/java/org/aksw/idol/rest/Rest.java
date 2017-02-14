/**
 * 
 */
package org.aksw.idol.rest;

import java.util.List;

import org.aksw.idol.core.exceptions.LODVaderMissingPropertiesException;
import org.aksw.idol.core.loader.LODVaderConfigurator;
import org.aksw.idol.core.loader.LODVaderProperties;
import org.aksw.idol.core.mongodb.collections.DistributionDB;
import org.aksw.idol.core.mongodb.collections.DistributionDB.DistributionStatus;
import org.aksw.idol.core.mongodb.queries.GeneralQueriesHelper;
import org.aksw.idol.core.streaming.LODVStreamFileImpl;
import org.aksw.idol.core.streaming.LODVStreamInterface;
import org.aksw.idol.core.tupleManager.processors.DBpediaProcessor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;




/**
 * @author Ciro Baron Neto
 * 
 *         Feb 14, 2017
 */ 
public class Rest {

	public static void main(String[] args) {
		/**
		 * Load properties file, create MondoDB indexes, etc
		 */
		LODVaderConfigurator s = new LODVaderConfigurator(); 
		s.configure();
 
		GeneralQueriesHelper queries = new GeneralQueriesHelper(); 

		// load distributions to be analyzed
		List<DBObject> distributionObjects = null;

		distributionObjects = queries.getObjects(DistributionDB.COLLECTION_NAME, new BasicDBObject());

		// for each object create a instance of distributionDB
		for (DBObject object : distributionObjects) {
			DistributionDB distribution = new DistributionDB(object);
			process(distribution);
		}
 
	}

	public static void process(DistributionDB distribution) {

		/**
		 * Check whether LODVader should stream from the internet of loca files
		 */
		LODVStreamInterface coreStream = null; 

		coreStream = new LODVStreamFileImpl(LODVaderProperties.BASE_PATH + "/raw_files/");

		/**
		 * Registering statistical data processor
		 */
		DBpediaProcessor dbpediaProcessor = null;
		coreStream.getPipelineProcessor().registerProcessor(dbpediaProcessor);

		// start processing
		try {
			distribution.setStatus(DistributionStatus.STREAMING);
			distribution.update();

			coreStream.startParsing(distribution);

			// after finishing processing, finalize the processors (save
			// data, etc etc).
			System.out.println(dbpediaProcessor.numberOfDbpediaSubject);
			System.out.println(dbpediaProcessor.numberOfDbpediaProperties);
			System.out.println(dbpediaProcessor.numberOfDbpediaObjects);

			distribution.setStatus(DistributionStatus.DONE);

		} catch (Exception e) {

			System.out.println("ERROR! Distribution: " + distribution.getDownloadUrl() + " has status "
					+ distribution.getStatus().toString() + " with error msg '" + distribution.getLastMsg() + "'.");
		}

		try {
			distribution.update();
		} catch (LODVaderMissingPropertiesException e) {
			e.printStackTrace();

		}

		System.out.println("Datasets to be processed: ");
	}

}
