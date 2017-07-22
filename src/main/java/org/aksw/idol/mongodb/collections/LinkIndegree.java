/**
 * 
 */
package org.aksw.idol.mongodb.collections;

import org.aksw.idol.mongodb.DBSuperClass;

import com.mongodb.DBObject;

/**
 * @author Ciro Baron Neto
 * 
 * Dec 4, 2016
 */
public class LinkIndegree extends DBSuperClass{

	public static String COLLECTION_NAME = "LinkIndegree";
	/**
	 * Constructor for Class LinkIndegree 
	 * @param collectionName
	 */
	public LinkIndegree() {
		super(COLLECTION_NAME);
	}
	
	public LinkIndegree(DBObject o ) {
		super(COLLECTION_NAME);
		mongoDBObject= o;
	}
	
	public static String DATASET = "dataset";
	public static String AMOUNT = "amount";

	public void setAmount(int amount) {
		addField(AMOUNT, amount);
	}
	
	
	public  void setdataset(String dataset) {
		addField(DATASET, dataset);
	}
	
	public  int getAmount() {
		return ((Number) getField(AMOUNT)).intValue();
	}
	
	
	public  String getDataset() {
		return getField(DATASET).toString();
	}
	
}
