package org.aksw.idol.properties;

public class DataSourcesProperties {
	public boolean dbpedia;

	public boolean lov;

	public boolean re3;

	public boolean ckanrepositories;

	public boolean sparqles;

	public boolean lodlaundromat;

	public boolean lodcloud;

	public boolean linghib;

	public boolean lodstats;

	public boolean isLov() {
		return lov;
	}

	public void setLov(boolean lov) {
		this.lov = lov;
	}

	public boolean isRe3() {
		return re3;
	}

	public void setRe3(boolean re3) {
		this.re3 = re3;
	}

	public boolean isCkanrepositories() {
		return ckanrepositories;
	}

	public void setCkanrepositories(boolean ckanrepositories) {
		this.ckanrepositories = ckanrepositories;
	}

	public boolean isSparqles() {
		return sparqles;
	}

	public void setSparqles(boolean sparqles) {
		this.sparqles = sparqles;
	}

	public boolean isLodlaundromat() {
		return lodlaundromat;
	}

	public void setLodlaundromat(boolean lodlaundromat) {
		this.lodlaundromat = lodlaundromat;
	}

	public boolean isLodcloud() {
		return lodcloud;
	}

	public void setLodcloud(boolean lodcloud) {
		this.lodcloud = lodcloud;
	}

	public boolean isLinghib() {
		return linghib;
	}

	public void setLinghib(boolean linghib) {
		this.linghib = linghib;
	}

	public boolean isLodstats() {
		return lodstats;
	}

	public void setLodstats(boolean lodstats) {
		this.lodstats = lodstats;
	}

	public boolean isDbpedia() {
		return dbpedia;
	}

	public void setDbpedia(boolean dbpedia) {
		this.dbpedia = dbpedia;
	}

}
