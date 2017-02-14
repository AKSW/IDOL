/**
 * 
 */
package org.aksw.idol.services.mongodb;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.aksw.idol.bloomfilters.BloomFilterI;
import org.aksw.idol.bloomfilters.impl.BloomFilterFactory;
import org.aksw.idol.mongodb.DBSuperClass;
import org.aksw.idol.mongodb.collections.datasetBF.BucketDB;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

/**
 * Some services for managing buckets
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 9, 2016
 */
public class BucketService {

	/**
	 * Get a set of bloom filters for a list of distributions
	 * 
	 * @param collection
	 *            the collection
	 * @param distributionsIDs
	 *            list of distribution ids
	 * @return a hashmap where the key if the distributionID and the values are
	 *         bloom filters
	 */
	public HashMap<String, List<BloomFilterI>> getDistributionFilters(BucketDB.COLLECTIONS collection,
			Collection<String> distributionsIDs) {

		// get all distributions within the dataset

		HashMap<String, List<BloomFilterI>> buckets = new HashMap<String, List<BloomFilterI>>();

		// get collection
		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), collection.toString());

		// create query
		BasicDBObject distributions = new BasicDBObject(BucketDB.DISTRIBUTION_ID,
				new BasicDBObject("$in", distributionsIDs));

		// make query
		List<GridFSDBFile> bucketsDB = gfs.find(distributions);

		for (GridFSDBFile f : bucketsDB) {
			BloomFilterI filter = BloomFilterFactory.newBloomFilter();
			try {
				filter.readFrom(f.getInputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (buckets.get(f.get(BucketDB.DISTRIBUTION_ID).toString()) == null) {
				List<BloomFilterI> list = new ArrayList<>();
				list.add(filter);
				buckets.put(f.get(BucketDB.DISTRIBUTION_ID).toString(), list);
			} else {
				buckets.get(f.get(BucketDB.DISTRIBUTION_ID).toString()).add(filter);
			}
		}

		return buckets;
	}

	/**
	 * Remove a bucket given a collection and a distribution
	 * 
	 * @param collection
	 *            the collection
	 * @param distributionID
	 *            the distribution id
	 */
	public void removeBucket(BucketDB.COLLECTIONS collection, String distributionID) {
		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), collection.toString());
		gfs.remove(new BasicDBObject(BucketDB.DISTRIBUTION_ID, distributionID));
	}

	/**
	 * Save a bucket into MongoDB
	 * 
	 * @param bucket
	 */
	public void saveBucket(BucketDB bucket) {

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		try {
			bucket.getBloomFilter().writeTo(out);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), bucket.COLLECTION.toString());
		GridFSInputFile gfsFile;

		try {
			gfsFile = gfs.createFile(new BufferedInputStream(new ByteArrayInputStream(out.toByteArray())));
			gfsFile.put(BucketDB.DISTRIBUTION_ID, bucket.getDistributionID());
			gfsFile.put(BucketDB.SEQUENCE_NR, bucket.getSequenceNr());
			gfsFile.put(BucketDB.SIZE, bucket.getSize());
			gfsFile.put(BucketDB.FPP, bucket.getFpp());
			gfsFile.put(BucketDB.FIRST, bucket.getFirst());
			gfsFile.put(BucketDB.LAST, bucket.getLast());
			gfsFile.save();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get a distribution bucket
	 * 
	 * @param collection
	 * @param distributionsID
	 * @return
	 */
	public BucketDB getBucket(BucketDB.COLLECTIONS collection, String distributionID, boolean loadBF) {
		BucketDB bucket = new BucketDB(collection);
		GridFS gfs = new GridFS(DBSuperClass.getDBInstance(), bucket.COLLECTION.toString());

		List<GridFSDBFile> gridFSObjects = gfs.find(new BasicDBObject(BucketDB.DISTRIBUTION_ID, distributionID));
		if (gridFSObjects.size() > 0) {
			GridFSDBFile gridFSObject = gridFSObjects.iterator().next();
			BloomFilterI filter = null;
			if (loadBF) {
				filter = BloomFilterFactory.newBloomFilter();
				try {
					filter.readFrom(gridFSObject.getInputStream());
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else
				filter = null;

			bucket.setBloomFilter(filter);
			bucket.setDistributionID(gridFSObject.get(BucketDB.DISTRIBUTION_ID).toString());
			bucket.setFirst(getWithNullHelper(gridFSObject, BucketDB.FIRST));
			bucket.setLast(getWithNullHelper(gridFSObject, BucketDB.LAST));
			bucket.setFpp(Double.parseDouble(gridFSObject.get(BucketDB.FPP).toString()));
			bucket.setSize(Double.parseDouble(gridFSObject.get(BucketDB.SIZE).toString()));
			bucket.setSequenceNr(Integer.parseInt(gridFSObject.get(BucketDB.SEQUENCE_NR).toString()));
			bucket.setBfByteSize(gridFSObject.getLength());
		}
		return bucket;
	}

	private String getWithNullHelper(GridFSDBFile gridFSObject, String field){
		try{ 
			return gridFSObject.get(BucketDB.LAST).toString();
		}
		catch (NullPointerException e)
		{
			return null;
		}
	}
	
}
