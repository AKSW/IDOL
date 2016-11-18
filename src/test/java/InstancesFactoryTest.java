import java.net.MalformedURLException;

import org.bson.types.ObjectId;
import org.openrdf.model.Statement;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionDB.DistributionStatus;
import lodVader.utils.StatementUtils;

/**
 * @author Ciro Baron Neto
 * 
 * Nov 14, 2016
 */
public class InstancesFactoryTest{
	
	static int nameID = 0;
	static int stmtID = 0;
	
	/**
	 * Creates an arbritrary distribution
	 * @return
	 */
	public static DistributionDB createDistribution(){
		DistributionDB distribution = new DistributionDB();
		int distributionN = nameID++;
		distribution.setID(ObjectId.get().toString());
		distribution.setTitle("Test distribution " + distributionN);
		try {
			distribution.setDownloadUrl("http://www.test.org/"+distributionN);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		distribution.setFormat("ttl");
		distribution.setStatus(DistributionStatus.WAITING_TO_STREAM);
		distribution.setIsVocabulary(false);
		distribution.setUri("http://www.test.org/"+distributionN);
		return distribution;
	}
	

	
}
