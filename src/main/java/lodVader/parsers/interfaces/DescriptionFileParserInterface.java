package lodVader.parsers.interfaces;

import java.util.List;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;


public interface DescriptionFileParserInterface {
	
	public List<DistributionDB> getDistributions();
	
	public List<DatasetDB> getDatasets();
	
	public void parse();

}
