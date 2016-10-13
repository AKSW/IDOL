package lodVader.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoCommandException;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DescriptionFileParserDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;
import lodVader.mongodb.collections.datasetBF.BucketDB;

public class IndexesCreator {

	public void createIndexes() {

		// indexes for datasets
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.PARENT_DATASETS, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.TITLE, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.URI, 1, true);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.SUBSET_IDS, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.PARENT_DATASETS, 1);

		// indexes for distributions
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DEFAULT_DATASETS, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DOWNLOAD_URL, 1, true);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.IS_VOCABULARY, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.STATUS, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DEFAULT_DATASETS, 1);

		addIndex(DescriptionFileParserDB.COLLECTION_NAME, DescriptionFileParserDB.REPOSITORY_ADDRESS, 1, true);

		for (GeneralResourceDB.COLLECTIONS collection : GeneralResourceDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), GeneralResourceDB.URI, 1, true);
		}
		 
		for (BucketDB.COLLECTIONS collection : BucketDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), BucketDB.DISTRIBUTION_ID, 1, true);
			addIndex(collection.toString(), BucketDB.SEQUENCE_NR, 1, true);
		}

		for (GeneralResourceRelationDB.COLLECTIONS collection : GeneralResourceRelationDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), GeneralResourceRelationDB.CUSTOM_ID, 1, true);
			addIndex(collection.toString(), GeneralResourceRelationDB.DATASET_ID, 1);
			addIndex(collection.toString(), GeneralResourceRelationDB.DISTRIBUTION_ID, 1);
			addIndex(collection.toString(), GeneralResourceRelationDB.PREDICATE_ID, 1);
		}

	}

	public void addIndex(String collection, String field, int value) {
		DBObject indexOptions = new BasicDBObject();
		indexOptions.put(field, value);
		DBSuperClass.getCollection(collection).createIndex(indexOptions);
	}

	public void addIndex(String collection, String field, int value, boolean unique) {
		try {
			DBObject indexOptions = new BasicDBObject();
			indexOptions.put(field, value);

			DBObject uniqueField = new BasicDBObject();
			uniqueField.put("unique", true);
			DBSuperClass.getCollection(collection).createIndex(indexOptions, uniqueField);
		} catch (MongoCommandException e) {

		}
	}

}
