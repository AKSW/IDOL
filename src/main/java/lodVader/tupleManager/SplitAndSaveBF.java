package lodVader.tupleManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.utils.Timer;
import lodVader.utils.bloomfilter.BloomFilterCache;

public class SplitAndSaveBF extends SuperTupleManager {

	final static Logger logger = LoggerFactory.getLogger(SplitAndSaveBF.class);

	public BloomFilterCache cache = new BloomFilterCache(200000, 0.00000001);
	

	@Override
	public void saveStatement(String stSubject, String stPredicate, String stObject) {

	
		
		if (stSubject.startsWith("http")) {
			stSubject = "<" + stSubject + ">";
		}
		if (stPredicate.startsWith("http")) {
			stPredicate = "<" + stPredicate + ">";
		}
		if (stObject.startsWith("http")) {
			stObject = "<" + stObject + ">";
			if(stObject.contains(" "))
				stObject = stObject.split(" <")[0];
		}
		
			


		try {

			cache.add(stSubject + " " + stPredicate + " " + stObject + " .");

			if (totalTriples % 1000000 == 0) {
				logger.info("Triples read: " + totalTriples + ", time: " + t.stopTimer());
				logger.info("Sample: "+stSubject + " " + stPredicate + " " + stObject + " .");
				
				t = new Timer();
				t.startTimer();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		totalTriples++;
		

	}


	/* (non-Javadoc)
	 * @see lodVader.tupleManager.SuperTupleManager#startFiles()
	 */
	@Override
	public void startFiles() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see lodVader.tupleManager.SuperTupleManager#closeFiles()
	 */
	@Override
	public void closeFiles() {
		// TODO Auto-generated method stub
		
	}

}
