package org.aksw.idol.properties;

import java.util.HashMap;
import java.util.Map;

public class TaskProperties {

	Map<String, Boolean> dumpcreation = new HashMap<String, Boolean>();

	Map<String, Boolean> bloomfiltercreation = new HashMap<String, Boolean>();

	public Boolean getCreateDumpOnDisk() {
		return dumpcreation.get("createDumpOnDisk");
	}

	public Boolean getOverrideDumpOnDisk() {
		return dumpcreation.get("overrideDumpOnDisk");
	}
	
	public Boolean getCreateDatasetsBloomFilter() {
		return bloomfiltercreation.get("createDatasetsBloomFilter");
	}
	
	public Boolean getCreateDatasourceBLoomFilter() {
		return bloomfiltercreation.get("createDataSourceBloomFilter");
	}
	public Boolean getOverrideCreatedBloomFilter() {
		return bloomfiltercreation.get("overrideCreatedBloomFilter");
	}
		

	boolean processstatisticaldata;
	
	CalculateUniqPerDataSourceProperties calculateUniqPerDataSource = new CalculateUniqPerDataSourceProperties();


	public CalculateUniqPerDataSourceProperties getCalculateUniqPerDataSource() {
		return calculateUniqPerDataSource;
	}

	public void setCalculateUniqPerDataSource(CalculateUniqPerDataSourceProperties calculateUniqPerDataSource) {
		this.calculateUniqPerDataSource = calculateUniqPerDataSource;
	}

	public Map<String, Boolean> getDumpcreation() {
		return dumpcreation;
	}

	public void setDumpcreation(Map<String, Boolean> dumpcreation) {
		this.dumpcreation = dumpcreation;
	}

	public Map<String, Boolean> getBloomfiltercreation() {
		return bloomfiltercreation;
	}

	public void setBloomfiltercreation(Map<String, Boolean> bloomfiltercreation) {
		this.bloomfiltercreation = bloomfiltercreation;
	}

	public boolean isProcessstatisticaldata() {
		return processstatisticaldata;
	}

	public void setProcessstatisticaldata(boolean processstatisticaldata) {
		this.processstatisticaldata = processstatisticaldata;
	}

}
