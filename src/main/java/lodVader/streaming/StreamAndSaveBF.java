package lodVader.streaming;

import java.io.IOException;

import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.datasetBF.DatasetBFBucketDB;
import lodVader.tupleManager.SplitAndSaveBF;

public class StreamAndSaveBF extends SuperStream {

	final static Logger logger = LoggerFactory.getLogger(StreamAndSaveBF.class);

	public void streamDistribution(DistributionDB distributionMongoDBObj)
			throws IOException, LODVaderLODGeneralException, InterruptedException, RDFHandlerException,
			RDFParseException, LODVaderFormatNotAcceptedException {
		
		
		SplitAndSaveBF tupleManager = new SplitAndSaveBF();
		setTupleManager(tupleManager);		
		startParsing(distributionMongoDBObj);  
		
		DatasetBFBucketDB bucket = new DatasetBFBucketDB();
		bucket.saveCache(tupleManager.cache, distributionMongoDBObj.getLODVaderID());
		
	}

}
