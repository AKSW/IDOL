package org.aksw.idol.properties;

import org.aksw.idol.properties.parsers.CKANParserProperties;
import org.aksw.idol.properties.parsers.DBpediaParserProperties;
import org.aksw.idol.properties.parsers.LODCloudParserProperties;
import org.aksw.idol.properties.parsers.LODLaundromatParserProperties;
import org.aksw.idol.properties.parsers.DatabusParserProperties;
import org.aksw.idol.properties.parsers.LOVParserProperties;
import org.aksw.idol.properties.parsers.LinghubParserProperties;
import org.aksw.idol.properties.parsers.RE3ParserProperties;
import org.aksw.idol.properties.parsers.SparqlesParserProperties;

public class ParseProperties {
	SparqlesParserProperties sparqles = new SparqlesParserProperties();
	LOVParserProperties lov = new LOVParserProperties();
	DBpediaParserProperties dbpedia = new DBpediaParserProperties();
	LODLaundromatParserProperties lodlaundromat = new LODLaundromatParserProperties();
	LODCloudParserProperties lodcloud = new LODCloudParserProperties();
	RE3ParserProperties re3 = new RE3ParserProperties();
	CKANParserProperties ckanrepositories = new CKANParserProperties();
	LinghubParserProperties linghub = new LinghubParserProperties();
	DatabusParserProperties databus = new DatabusParserProperties();
	public SparqlesParserProperties getSparqles() {
		return sparqles;
	}
	public void setSparqles(SparqlesParserProperties sparqles) {
		this.sparqles = sparqles;
	}
	public LOVParserProperties getLov() {
		return lov;
	}
	public void setLov(LOVParserProperties lov) {
		this.lov = lov;
	}
	public DBpediaParserProperties getDbpedia() {
		return dbpedia;
	}
	public void setDbpedia(DBpediaParserProperties dbpedia) {
		this.dbpedia = dbpedia;
	}
	public LODLaundromatParserProperties getLodlaundromat() {
		return lodlaundromat;
	}
	public void setLodlaundromat(LODLaundromatParserProperties lodlaundromat) {
		this.lodlaundromat = lodlaundromat;
	}
	public LODCloudParserProperties getLodcloud() {
		return lodcloud;
	}
	public void setLodcloud(LODCloudParserProperties lodcloud) {
		this.lodcloud = lodcloud;
	}
	public RE3ParserProperties getRe3() {
		return re3;
	}
	public void setRe3(RE3ParserProperties re3) {
		this.re3 = re3;
	}

	public CKANParserProperties getCkanrepositories() {
		return ckanrepositories;
	}
	public void setCkanrepositories(CKANParserProperties ckanrepositories) {
		this.ckanrepositories = ckanrepositories;
	}
	public LinghubParserProperties getLinghub() {
		return linghub;
	}
	public void setLinghub(LinghubParserProperties linghub) {
		this.linghub = linghub;
	}
	public DatabusParserProperties getDatabus() {
		return databus;
	}
	public void setDatabus(DatabusParserProperties databus) {
		this.databus = databus;
	}

}
