package lodVader.mongodb.collections.datasetBF;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.mongodb.DBSuperClass;

public class BucketDB {

	final static Logger logger = LoggerFactory.getLogger(BucketDB.class);

	public COLLECTIONS COLLECTION;

	public static enum COLLECTIONS {
		BLOOM_FILTER_SUBJECTS, BLOOM_FILTER_OBJECTS, BLOOM_FILTER_TRIPLES
	};

	static public String SEQUENCE_NR = "sequenceNr";

	static public String DISTRIBUTION_ID = "distributionID";

	static public String SIZE = "size";

	static public String FIRST = "first";

	static public String LAST = "last";

	static public String FPP = "fpp";

	/**
	 * Constructor for Class BucketDB
	 */
	public BucketDB(COLLECTIONS collection) {
		this.COLLECTION = collection;
	}

	public void saveBF(BloomFilterI bf, String distributionID, int bfSequenceNr, String first, String last) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		try {
			bf.writeTo(out);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), COLLECTION.toString());
		GridFSInputFile gfsFile;

		try {
			gfsFile = gfs.createFile(new BufferedInputStream(new ByteArrayInputStream(out.toByteArray())));
			gfsFile.put(DISTRIBUTION_ID, distributionID);
			gfsFile.put(SEQUENCE_NR, bfSequenceNr);
			gfsFile.put(SIZE, bf.getFilterInitialSize());
			gfsFile.put(FPP, bf.getFPP());
			gfsFile.put(FIRST, first);
			gfsFile.put(LAST, last);
			gfsFile.save();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void remove(String distributionID) {
		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), COLLECTION.toString());
		gfs.remove(new BasicDBObject(DISTRIBUTION_ID, distributionID));
	}

	// public boolean query(int distributionID) {
	//
	// GridFS gfsFile = new GridFS(DBSuperClass.getDBInstance(),
	// COLLECTION_NAME);
	//
	// boolean result = false;
	//
	// BasicDBObject firstResource = new BasicDBObject(FIRST_RESOURCE, new
	// BasicDBObject("$lte", resource));
	// BasicDBObject lastResource = new BasicDBObject(LAST_RESOURCE, new
	// BasicDBObject("$gte", resource));
	// BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID,
	// distributionID);
	//
	// BasicDBList and = new BasicDBList();
	// and.add(firstResource);
	// and.add(lastResource);
	// and.add(distribution);
	//
	// GridFSDBFile file = gfsFile.findOne(new BasicDBObject("$and", and));
	//
	// // Timer t = new Timer();
	// // t.startTimer();
	// BloomFilterI filter = BloomFilterFactory.newBloomFilter();
	// if (file != null)
	// try {
	// filter.readFrom(file.getInputStream());
	// result = filter.compare(resource);
	//
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// // time=time+Double.parseDouble(t.stopTimer());
	// // System.out.println(time);
	//
	// return result;
	// }

	// public BloomFilterCache loadCache(int distributionID) throws IOException
	// {
	//
	// BloomFilterCache bfCache = null;
	//
	// GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), COLLECTION_NAME);
	//
	// BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID,
	// distributionID);
	//
	// List<GridFSDBFile> caches = gfs.find(distribution);
	//
	// for (GridFSDBFile cache : caches) {
	// BloomFilterI filter = BloomFilterFactory.newBloomFilter();
	//
	// if (bfCache == null) {
	// filter.readFrom(cache.getInputStream());
	// bfCache = new
	// BloomFilterCache(Integer.parseInt(cache.get(SIZE).toString()),
	// Double.parseDouble(cache.get(FPP).toString()), filter);
	// } else {
	// filter.readFrom(cache.getInputStream());
	// bfCache.getListOfBF().add(filter);
	// }
	// }
	//
	// return bfCache;
	//
	// }
	//


}
