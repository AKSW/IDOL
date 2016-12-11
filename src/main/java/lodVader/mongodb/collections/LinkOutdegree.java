/**
 * 
 */
package lodVader.mongodb.collections;

import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass;

/**
 * @author Ciro Baron Neto
 * 
 * Dec 4, 2016
 */
public class LinkOutdegree extends DBSuperClass{
	
	public static String COLLECTION_NAME = "LinkOutdegree";

	/**
	 * Constructor for Class LinkIndegree 
	 * @param collectionName
	 */
	public LinkOutdegree() {
		super(COLLECTION_NAME);
	}
	
	public LinkOutdegree(DBObject o) {
		super(COLLECTION_NAME);
		mongoDBObject = o;
	}
	
	public static String DATSET = "dataset";
	public static String AMOUNT = "amount";

	public void setAmount(int amount) {
		addField(AMOUNT, amount);
	}
	
	
	public  void setdataset(String dataset) {
		addField(DATSET, dataset);
	}
	
	public  int getAmount() {
		return ((Number) getField(AMOUNT)).intValue();
	}
	
	
	public  String getDataset() {
		return getField(DATSET	).toString();
	}
	
}
