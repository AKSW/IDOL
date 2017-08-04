package org.aksw.idol.properties;

import java.util.HashMap;
import java.util.Map;

public class TaskProperties {

	Map<String, Boolean> dumpcreation = new HashMap<String, Boolean>();

	Map<String, Boolean> bloomfiltercreation = new HashMap<String, Boolean>();

	public Boolean getCreateDumpOnDisk() {
		return dumpcreation.get("createdumpondisk");
	}

	public Boolean getOverrideDumpOnDisk() {
		return dumpcreation.get("overridedumpondisk");
	}
	
	public Boolean getCreateDatasetsBloomFilter() {
		return bloomfiltercreation.get("createdatsetsbloomfilter");
	}
	
	public Boolean getCreateDatasourceBLoomFilter() {
		return bloomfiltercreation.get("createdatasourcebloomfilter");
	}
	public Boolean getOverrideCreatedBloomFilter() {
		return bloomfiltercreation.get("overridecreatedbloomfilter");
	}
		

	boolean processstatisticaldata;
	
	boolean calculateuniqperdatasource;

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

	public boolean isCalculateuniqperdatasource() {
		return calculateuniqperdatasource;
	}

	public void setCalculateuniqperdatasource(boolean calculateuniqperdatasource) {
		this.calculateuniqperdatasource = calculateuniqperdatasource;
	}
	
	

}
