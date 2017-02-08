/**
 * 
 */
package idol.mongodb.collections;

import idol.mongodb.DBSuperClass;

/**
 * @author Ciro Baron Neto
 * 
 * Dec 4, 2016
 */
public class LinkOutdegree extends DBSuperClass{

	/**
	 * Constructor for Class LinkIndegree 
	 * @param collectionName
	 */
	public LinkOutdegree() {
		super("LinkIndegree");
	}
	
	static String DATSET = "dataset";
	static String AMOUNT = "amount";

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
