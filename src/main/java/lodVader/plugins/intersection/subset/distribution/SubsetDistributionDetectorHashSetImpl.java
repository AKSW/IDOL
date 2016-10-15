/**
 * 
 */
package lodVader.plugins.intersection.subset.distribution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.loader.LODVaderProperties;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.plugins.LODVaderPlugin;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;
import lodVader.streaming.LODVaderCoreStream;
import lodVader.tupleManager.processors.SaveRawDataProcessor;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 11, 2016
 */
public class SubsetDistributionDetectorHashSetImpl extends LODVaderIntersectionPlugin{
	

	public final static String PLUGIN_NAME = "SUBSET_HASH_SET_DETECTOR";

	/**
	 * Constructor for Class SubsetDetectorHashSetImpl 
	 * @param pluginName
	 */
	public SubsetDistributionDetectorHashSetImpl() {
		super(PLUGIN_NAME);
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * lodVader.application.subsetdetection.SubsetDetectionI#detectSubsets()
	 */
	@Override
	public HashMap<String, Double> runDetection(DistributionDB sourceDistribution,
			List<String> targetDistributionsIDs) {
		
		HashMap<String, Double> returnMap = new HashMap<String, Double>();


		String prefix = "dist_";

		String fileName = prefix + sourceDistribution.getID();

		HashSet<String> sourceSet = loadSetFromDisk(fileName);

		if (sourceSet == null) {
			LODVaderCoreStream stream = new LODVaderCoreStream();

			SaveRawDataProcessor processor = new SaveRawDataProcessor(sourceDistribution, fileName);

			stream.getPipelineProcessor().registerProcessor(processor);
			try {
				stream.startParsing(sourceDistribution);
			} catch (IOException | LODVaderLODGeneralException | LODVaderFormatNotAcceptedException e) {
				e.printStackTrace();
			}

		} else {

			for (String targetDistributionID : targetDistributionsIDs) {
				String targetDistributionFileName = prefix + targetDistributionID;
				HashSet<String> targetSet = loadSetFromDisk(targetDistributionFileName);

				DistributionDB targetDistribution = new DistributionDB();
				targetDistribution.find(true, DistributionDB.ID, targetDistributionID);

				if (targetSet == null) {
					LODVaderCoreStream stream = new LODVaderCoreStream();

					SaveRawDataProcessor processor = new SaveRawDataProcessor(targetDistribution,
							targetDistributionFileName);
					
					stream.getPipelineProcessor().registerProcessor(processor);
					try {
						stream.startParsing(targetDistribution);
					} catch (IOException | LODVaderLODGeneralException | LODVaderFormatNotAcceptedException e) {
						e.printStackTrace();
					}
					targetSet = loadSetFromDisk(targetDistributionFileName);

				}
				Double commonTriples = compareTwoSets(sourceSet, targetSet);
				if (commonTriples > 0.0) {
					returnMap.put(targetDistributionID, commonTriples);
				}

			}

		}
		
		return returnMap;

	}

	private HashSet<String> loadSetFromDisk(String setID) {
		File file = new File(LODVaderProperties.TMP_FOLDER + setID);
		if (file.exists()) {
			try {

				HashSet<String> r = new HashSet<>();
				new BufferedReader(new FileReader(file)).lines().forEach((line) -> {
					r.add(line);
				});

				return r;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}

		} else
			return null;
	}

	private Double compareTwoSets(HashSet<String> setA, HashSet<String> setB) {
		Double counter = 0.0;
		for (String s : setA) {
			if (setB.contains(s))
				counter++;
		}
		return counter;
	}

}