package lodVader.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoCommandException;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinkIndegree;
import lodVader.mongodb.collections.LinkOutdegree;
import lodVader.mongodb.collections.MetadataParserDB;
import lodVader.mongodb.collections.Resources.GeneralResourceDB;
import lodVader.mongodb.collections.Resources.GeneralResourceRelationDB;
import lodVader.mongodb.collections.ckanparser.CkanCatalogDB;
import lodVader.mongodb.collections.ckanparser.CkanDatasetDB;
import lodVader.mongodb.collections.ckanparser.CkanResourceDB;
import lodVader.mongodb.collections.datasetBF.BucketDB;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;

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

		


		addIndex(CkanCatalogDB.COLLECTION_NAME, CkanCatalogDB.CATALOG_URL, 1, true);
		addIndex(CkanDatasetDB.COLLECTION_NAME, CkanDatasetDB.CKAN_ID, 1, true);
		addIndex(CkanResourceDB.COLLECTION_NAME, CkanDatasetDB.CKAN_ID, 1,true);

		addIndex(LinkOutdegree.COLLECTION_NAME, LinkOutdegree.DATSET , 1,true); 
		addIndex(LinkIndegree.COLLECTION_NAME, LinkIndegree.DATASET , 1,true);  

		 
		
		addIndex("PLUGIN_INTERSECTION_PLUGIN", LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString(), 1);
		addIndex("PLUGIN_INTERSECTION_PLUGIN", LODVaderIntersectionPlugin.TARGET_DISTRIBUTION.toString(), 1);
		addIndex("PLUGIN_INTERSECTION_PLUGIN", LODVaderIntersectionPlugin.VALUE.toString(), 1);

		addIndex(MetadataParserDB.COLLECTION_NAME, MetadataParserDB.PARSER_NAME, 1, true);

		for (GeneralResourceDB.COLLECTIONS collection : GeneralResourceDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), GeneralResourceDB.URI, 1, true);
		}
		 
		for (BucketDB.COLLECTIONS collection : BucketDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), BucketDB.DISTRIBUTION_ID, 1);
//			addIndex(collection.toString(), BucketDB.SEQUENCE_NR, 1);
		}

		for (GeneralResourceRelationDB.COLLECTIONS collection : GeneralResourceRelationDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), GeneralResourceRelationDB.DISTRIBUTION_ID, 1 );
			addIndex(collection.toString(), GeneralResourceRelationDB.PREDICATE_ID, 1);
			addIndex(collection.toString(), GeneralResourceRelationDB.DATASET_ID, 1);
			DBObject indexFields = new BasicDBObject();
			indexFields.put(GeneralResourceRelationDB.DISTRIBUTION_ID, 1);
			indexFields.put(GeneralResourceRelationDB.PREDICATE_ID, 1);
	
			addIndex(collection.toString(), indexFields, true);
			
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
	
	public void addIndex(String collection, DBObject obj, boolean unique) {
		try {
			DBObject uniqueField = new BasicDBObject();
			uniqueField.put("unique", true);
			DBSuperClass.getCollection(collection).createIndex(obj, uniqueField);
		} catch (MongoCommandException e) {

		}
	}

}
