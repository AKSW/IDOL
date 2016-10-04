package lodVader.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoCommandException;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DatasetLinksetDB;
import lodVader.mongodb.collections.DescriptionFileParserDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceDB;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import lodVader.mongodb.collections.gridFS.SuperBucket;
import lodVader.mongodb.collections.namespaces.DistributionObjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.mongodb.collections.toplinks.TopInvalidLinks;
import lodVader.mongodb.collections.toplinks.TopValidLinks;

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
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.URI, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DEFAULT_DATASETS, 1);

		addIndex(DescriptionFileParserDB.COLLECTION_NAME, DescriptionFileParserDB.REPOSITORY_ADDRESS, 1, true);

		// indexes for datasetsLinksets
		addIndex(DatasetLinksetDB.COLLECTION_NAME, DatasetLinksetDB.LINKSET_ID, 1);
		addIndex(DatasetLinksetDB.COLLECTION_NAME, DatasetLinksetDB.DATASET_SOURCE, 1);
		addIndex(DatasetLinksetDB.COLLECTION_NAME, DatasetLinksetDB.DATASET_TARGET, 1);
		addIndex(DatasetLinksetDB.COLLECTION_NAME, DatasetLinksetDB.DISTRIBUTION_SOURCE, 1);
		addIndex(DatasetLinksetDB.COLLECTION_NAME, DatasetLinksetDB.DISTRIBUTION_TARGET, 1);
		addIndex(DatasetLinksetDB.COLLECTION_NAME, DatasetLinksetDB.LINKS, 1);
		addIndex(DatasetLinksetDB.COLLECTION_NAME, DatasetLinksetDB.DEAD_LINKS, 1);

		// indexes for linksets
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.LINKSET_ID, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DATASET_SOURCE, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DATASET_TARGET, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DISTRIBUTION_SOURCE, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DISTRIBUTION_TARGET, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.LINK_NUMBER_LINKS, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.PREDICATE_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.RDF_TYPE_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.RDF_SUBCLASS_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.OWL_CLASS_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.LINK_STRENGHT, 1);

		for (GeneralRDFResourceDB.COLLECTIONS collection : GeneralRDFResourceDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), GeneralRDFResourceDB.URI, 1, true);
		}

		for (GeneralRDFResourceRelationDB.COLLECTIONS collection : GeneralRDFResourceRelationDB.COLLECTIONS.values()) {
			addIndex(collection.toString(), GeneralRDFResourceRelationDB.CUSTOM_ID, 1, true);
			addIndex(collection.toString(), GeneralRDFResourceRelationDB.DATASET_ID, 1);
			addIndex(collection.toString(), GeneralRDFResourceRelationDB.DISTRIBUTION_ID, 1);
			addIndex(collection.toString(), GeneralRDFResourceRelationDB.PREDICATE_ID, 1);
		}

		addIndex(DistributionSubjectNS0DB.COLLECTION_NAME, DistributionSubjectNS0DB.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectNS0DB.COLLECTION_NAME, DistributionSubjectNS0DB.DATASET_ID, 1);
		addIndex(DistributionSubjectNS0DB.COLLECTION_NAME, DistributionSubjectNS0DB.NS, 1);

		addIndex(DistributionObjectNS0DB.COLLECTION_NAME, DistributionObjectNS0DB.DISTRIBUTION_ID, 1);
		addIndex(DistributionObjectNS0DB.COLLECTION_NAME, DistributionObjectNS0DB.DATASET_ID, 1);
		addIndex(DistributionObjectNS0DB.COLLECTION_NAME, DistributionObjectNS0DB.NS, 1);

		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.DATASET_ID, 1);
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.NS, 1);

		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.DATASET_ID, 1);
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.NS, 1);

		addIndex(TopInvalidLinks.COLLECTION_NAME, TopInvalidLinks.SOURCE_DISTRIBUTION_ID, 1);
		addIndex(TopInvalidLinks.COLLECTION_NAME, TopInvalidLinks.TARGET_DISTRIBUTION_ID, 1);
		addIndex(TopInvalidLinks.COLLECTION_NAME, TopInvalidLinks.AMOUNT, 1);

		addIndex(TopValidLinks.COLLECTION_NAME, TopValidLinks.SOURCE_DISTRIBUTION_ID, 1);
		addIndex(TopValidLinks.COLLECTION_NAME, TopValidLinks.TARGET_DISTRIBUTION_ID, 1);
		addIndex(TopValidLinks.COLLECTION_NAME, TopValidLinks.AMOUNT, 1);

		// indices for gridFS
		addIndex("ObjectsBucket.files", SuperBucket.DISTRIBUTION_ID, 1);
		addIndex("ObjectsBucket.files", SuperBucket.FIRST_RESOURCE, 1);
		addIndex("ObjectsBucket.files", SuperBucket.LAST_RESOURCE, 1);

		addIndex("SubjectsBucket.files", SuperBucket.DISTRIBUTION_ID, 1);
		addIndex("SubjectsBucket.files", SuperBucket.FIRST_RESOURCE, 1);
		addIndex("SubjectsBucket.files", SuperBucket.LAST_RESOURCE, 1);
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
