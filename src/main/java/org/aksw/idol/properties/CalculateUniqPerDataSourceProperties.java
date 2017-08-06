package org.aksw.idol.properties;

public class CalculateUniqPerDataSourceProperties {
	
	public DataSourcesProperties dataSources = new DataSourcesProperties();
	
	public long bloomFilterSize;
	
	public double bloomFilterFpp;
	
	String tmpDir;
	

	public long getBloomFilterSize() {
		return bloomFilterSize;
	}

	public void setBloomFilterSize(long bloomFilterSize) {
		this.bloomFilterSize = bloomFilterSize;
	}

	public double getBloomFilterFpp() {
		return bloomFilterFpp;
	}

	public void setBloomFilterFpp(double bloomFilterFpp) {
		this.bloomFilterFpp = bloomFilterFpp;
	}

	public String getTmpDir() {
		return tmpDir;
	}

	public void setTmpDir(String tmpDir) {
		this.tmpDir = tmpDir;
	}

	public DataSourcesProperties getDataSources() {
		return dataSources;
	}

	public void setDataSources(DataSourcesProperties dataSources) {
		this.dataSources = dataSources;
	}

	

}
