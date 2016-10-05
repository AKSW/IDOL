package lodVader.mongodb.collections.datasetBF;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.bloomfilters.impl.BloomFilterOrestesImpl;
import lodVader.mongodb.DBSuperClass;
import lodVader.utils.bloomfilter.BloomFilterCache;
import orestes.bloomfilter.BloomFilter;

public class BucketDB {

	final static Logger logger = LoggerFactory.getLogger(BucketDB.class);
	
	private COLLECTIONS COLLECTION;
	
	public static enum COLLECTIONS {BLOOM_FILTER_SUBJECTS, BLOOM_FILTER_OBJECTS, BLOOM_FILTER_TRIPLES};

	static public String SEQUENCE_NR = "sequenceNr";

	static public String DISTRIBUTION_ID = "distributionID";

	static public String SIZE = "size";

	static public String FPP = "fpp";
	
	/**
	 * Constructor for Class BucketDB 
	 */
	public BucketDB(COLLECTIONS collection) {
		this.COLLECTION = collection;
	}
	

	public void saveBF(BloomFilterI bf, String distributionID, int bfSequenceNr) {
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
			gfsFile.save();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

//	public void saveCache(BloomFilterCache cache, int distributionID) {
//		int i = 0;
//		for (BloomFilterI bf : cache.getListOfBF()) {
//			saveBF(bf, distributionID, i++, cache.getInitialSize(), cache.getFpp());
//		}
//	}

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

//	public BloomFilterCache loadCache(int distributionID) throws IOException {
//
//		BloomFilterCache bfCache = null;
//
//		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), COLLECTION_NAME);
//
//		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);
//
//		List<GridFSDBFile> caches = gfs.find(distribution);
//
//		for (GridFSDBFile cache : caches) {
//			BloomFilterI filter = BloomFilterFactory.newBloomFilter();
//
//			if (bfCache == null) {
//				filter.readFrom(cache.getInputStream());
//				bfCache = new BloomFilterCache(Integer.parseInt(cache.get(SIZE).toString()),
//						Double.parseDouble(cache.get(FPP).toString()), filter);
//			} else {
//				filter.readFrom(cache.getInputStream());
//				bfCache.getListOfBF().add(filter);
//			}
//		}
//
//		return bfCache;
//
//	}
	//
	// public ArrayList<DatasetBFBucketDB> getFiltersFromDataset(int datasetID)
	// {
	//
	// // get all distributions within the dataset
	// ArrayList<Integer> distributionsIDs = new
	// DatasetQueries().getDistributionsIDs(datasetID);
	//
	// ArrayList<DatasetBFBucketDB> result = new ArrayList<DatasetBFBucketDB>();
	//
	// // get collection
	// GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), COLLECTION_NAME);
	//
	// // create query
	// BasicDBObject in = new BasicDBObject("$in", distributionsIDs);
	//
	// BasicDBObject distributions = new BasicDBObject(DISTRIBUTION_ID, in);
	//
	// // make query
	// List<GridFSDBFile> buckets = gfs.find(distributions);
	//
	// for (GridFSDBFile f : buckets) {
	// BloomFilterI filter = BloomFilterFactory.newBloomFilter();
	// try {
	// filter.readFrom(f.getInputStream());
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// result.add(
	// new DatasetBFBucketDB(filter, f.get(FIRST_RESOURCE).toString(),
	// f.get(LAST_RESOURCE).toString()));
	// }
	//
	// return result;
	// }

}
