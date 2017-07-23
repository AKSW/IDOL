package org.aksw.idol.properties;

import java.util.HashMap;
import java.util.Map;

public class TaskProperties {

	Map<String, Boolean> dumpcreation = new HashMap<String, Boolean>();

	Map<String, String> bloomfiltercreation = new HashMap<String, String>();

	public Boolean getCreateDumpOnDisk() {
		return dumpcreation.get("createdumpondisk");
	}

	public Boolean getOverrideDumpOnDisk() {
		return dumpcreation.get("overridedumpondisk");
	}
	
	public Boolean getCreateDatasetsBloomFilter() {
		return dumpcreation.get("createdatsetsbloomfilter");
	}
	
	public Boolean getCreateDatasourceBLoomFilter() {
		return dumpcreation.get("createdatasourcebloomfilter");
	}
	public Boolean getOverrideCreatedBloomFilter() {
		return dumpcreation.get("overridecreatedbloomfilter");
	}
		

	boolean processstatisticaldata;
	
	boolean calculateuniqperdatasource;

	public Map<String, Boolean> getDumpcreation() {
		return dumpcreation;
	}

	public void setDumpcreation(Map<String, Boolean> dumpcreation) {
		this.dumpcreation = dumpcreation;
	}

	public Map<String, String> getBloomfiltercreation() {
		return bloomfiltercreation;
	}

	public void setBloomfiltercreation(Map<String, String> bloomfiltercreation) {
		this.bloomfiltercreation = bloomfiltercreation;
	}

	public boolean isProcessstatisticaldata() {
		return processstatisticaldata;
	}

	public void setProcessstatisticaldata(boolean processstatisticaldata) {
		this.processstatisticaldata = processstatisticaldata;
	}

	public boolean isCalculateuniqperdatasource() {
		return calculateuniqperdatasource;
	}

	public void setCalculateuniqperdatasource(boolean calculateuniqperdatasource) {
		this.calculateuniqperdatasource = calculateuniqperdatasource;
	}
	
	

}
