/**
 * 
 */
package lodVader.parsers.descriptionFileParser.helpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.services.mongodb.DatasetServices;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 24, 2016
 */
public class SubsetHelper {
	/**
	 * Organize datasets distributions by URI. For example, the dataset with the
	 * downloadURL http://example.org/path1/path2 is a subset of the
	 * http://example.org/path1 which is a distribution of the dataset
	 * http://example.org/
	 */

	public void rearrangeSubsets(Collection<DistributionDB> distributions, HashMap<String, DatasetDB> datasets) {

		// sort the distributions by URI
		Collections.sort(new ArrayList<DistributionDB>(distributions), new Comparator<DistributionDB>() {
			public int compare(DistributionDB a, DistributionDB b) {
				return a.getDownloadUrl().compareTo(b.getDownloadUrl());
			}
		});

		HashSet<String> removeSet = new HashSet<String>();

		DatasetDB tmpDataset = null;
		for (DistributionDB distribution : distributions) {
			if (tmpDataset == null) {
				tmpDataset = datasets.get(distribution.getTopDatasetID());
			} else {
				if (distribution.getUri().startsWith(tmpDataset.getUri())) {
					try {
						tmpDataset.addDistributionID(distribution.getID());
						tmpDataset.update();

						// removeSet.add(tmpDataset.getID());

						distribution.setTopDataset(tmpDataset.getID());
						distribution.update();

					} catch (LODVaderMissingPropertiesException e) {
						e.printStackTrace();
					}

				} else {
					tmpDataset.find(true, DatasetDB.ID, distribution.getTopDatasetID());
				}
			}
		}

		// remove all datasets that don't contain distributions
		for (String i : removeSet) {
			new DatasetServices().removeDataset(i);
			datasets.remove(i);
		}
	}

}
