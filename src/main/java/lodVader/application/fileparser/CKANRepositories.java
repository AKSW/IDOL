/**
 * 
 */
package lodVader.application.fileparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.parsers.descriptionFileParser.DescriptionFileParserLoader;
import lodVader.parsers.descriptionFileParser.Impl.CKANParser;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class CKANRepositories {

	final static Logger logger = LoggerFactory.getLogger(CKANRepositories.class);

	// number of concurrent request to be made for each repository
	final int numberOfConcurrentRequests = 5;

	// number of repositories to be analyzed concurrently
	final int numberOfConcurrentRepositories = 3;
//	static ArrayList<String> ckanRepositories = new ArrayList<>(Arrays.asList("http://africaopendata.org/"));
//	static ArrayList<String> ckanRepositories = new ArrayList<>(Arrays.asList("http://drdsi.jrc.ec.europa.eu"));
	
	static ArrayList<String> ckanRepositories = new ArrayList<>(Arrays.asList("http://africaopendata.org/",
			"http://dados.al.gov.br", "https://open.alberta.ca", "http://www.amsterdamopendata.nl",
			"http://annuario.comune.fi.it", "http://opendata.aragon.es/", "http://go.arenysdemunt.cat/ca/dataset",
			"http://datos.santander.es", "https://data.barrowbc.gov.uk", "http://catalogue.data.gov.bc.ca/dataset",
			"http://daten.berlin.de", "http://bermuda.io/", "http://data.buenosaires.gob.ar/",
			"https://catalogodatos.gub.uy/", "http://datos.gov.py", "http://cities.opendatahub.gr",
			"http://data.ottawa.ca/", "http://data.surrey.ca/", "http://data.zagreb.hr", "http://www.civicdata.io",
			"https://datahub.cmap.illinois.gov", "http://opendata.cmt.es/", "http://datos.codeandomexico.org/",
			"http://data.kk.dk/", "http://dados.ima.sp.gov.br", "http://dados.recife.pe.gov.br/", "http://dados.gov.br",
			"http://dados.rs.gov.br/", "http://drdsi.jrc.ec.europa.eu", "http://dartportal.leeds.ac.uk/",
			"http://datar.noip.me", "http://ckan.sabae.jrrk.org", "http://dataforjapan.org", "http://data.suwon.go.kr",
			"http://data.bris.ac.uk/data", "http://data.gc.ca", "http://www.data.go.jp", "http://data.gov",
			"http://data.gov.au", "http://data.gov.ie", "http://data.gov.ro/", "http://data.gov.sk",
			"http://data.gov.uk", "http://data.gv.at", "http://data.nsw.gov.au", "http://data.overheid.nl",
			"http://data.rio.rj.gov.br/", "http://data.salzburgerland.com", "http://data.vic.gov.au/data",
			"http://data.wa.gov.au", "http://datacatalogs.org/", "http://datagm.org.uk", "http://datamx.io",
			"http://datapoa.com.br/", "http://dati.gov.it/", "http://datosabiertos.malaga.eu/", "http://datos.gob.mx/",
			"http://datosabiertos.ec", "http://data.denvergov.org/", "http://ecaidata.org", "http://www.ecds.se",
			"http://data.edostate.gov.ng/", "https://edx.netl.doe.gov", "http://opendata.riik.ee",
			"http://etsin.avointiede.fi", "http://open-data.europa.eu/", "http://www.europeandataportal.eu/data",
			"http://dados.fortaleza.ce.gov.br", "http://ckan.gsi.go.jp", "http://data.glasgow.gov.uk/",
			"http://gong.io", "http://govdata.de", "http://data.grcity.us", "http://data.graz.gv.at/",
			"http://opendatahub.gr", "http://www.hri.fi", "http://data.ohouston.org/", "https://hdx.rwlabs.org",
			"http://www.opendata.provincia.roma.it/", "http://data.go.id", "http://130.179.67.140",
			"http://datastore.landcareresearch.co.nz", "http://data.lexingtonky.gov/", "http://data.linz.gv.at/",
			"http://gisdata.mn.gov", "http://donnees.ville.montreal.qc.ca/", "http://data.nantou.gov.tw",
			"http://geothermaldata.org", "https://data.noaa.gov/dataset", "http://www.nosdonnees.fr/",
			"http://www.offene-daten.me", "http://offenedaten.de/", "http://www.odaa.dk/", "http://www.odaa.dk/",
			"http://data.gov.bf", "http://opendatacanarias.es/", "http://dati.toscana.it/",
			"http://opendatagortynia.gr/", "http://www.opendatahub.it/", "http://data.ug/", "http://dati.lazio.it",
			"http://datameti.go.jp", "http://opendata.aachen.de", "http://daten.hamburg.de/", "http://data.gov.hr",
			"http://dati.trentino.it/", "http://data.openva.com/", "http://portal.openbelgium.be",
			"http://opencolorado.org/", "http://opendata.caceres.es/", "http://opendata.lisra.jp",
			"http://opendata.awt.be/", "https://opendata.gov.je", "http://www.opendata-hro.de/", "http://opendata.hu/",
			"http://data.wu.ac.at", "https://www.opendatabc.ca", "http://opendatadc.org/", "https://opendataportal.at",
			"http://opendatareno.org", "http://dati.openexpo2015.it", "http://hubofdata.ru/", "http://www.openumea.se/",
			"http://opingogn.is/", "http://dados.prefeitura.sp.gov.br", "http://www.datos.misiones.gov.ar",
			"http://datospublicos.gob.ar/", "http://opendata.comune.bari.it/", "http://publicdata.eu",
			"http://data.qld.gov.au", "http://catalogue.datalocale.fr", "http://www.daten.rlp.de/",
			"http://rotterdamopendata.nl/", "http://data.cityofsantacruz.com/", "http://donnees.ville.sherbrooke.qc.ca",
			"http://data.sa.gov.au/", "https://opendata.swiss/", "http://taijiang.tw", "http://data.tainan.gov.tw",
			"http://dadosabertos.senado.gov.br/", "http://datahub.io/", "http://iatiregistry.org",
			"http://data.london.gov.uk", "http://data-gov-ua.org", "http://udct-data.aigid.jp",
			"http://data.yokohamaopendata.jp", "http://oppnadata.se"));

	
	public void loadAllRepositories() {

		ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentRepositories);
		ckanRepositories.forEach((repo) -> {
			executor.execute(new HttpRepositoryRequestThread(repo));
		});

		executor.shutdown();
		try {
			executor.awaitTermination(300, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("Loaded all CKAN repositories.");
		
	}

	/**
	 * Load many CKAN repositories concurrently
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Oct 1, 2016
	 */
	class HttpRepositoryRequestThread implements Runnable {

		String repository;

		CKANParser ckanParser;

		// CkanClient client;

		public HttpRepositoryRequestThread(String repository) {
			this.repository = repository;
			ckanParser = new CKANParser(repository, numberOfConcurrentRequests);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			DescriptionFileParserLoader parser = new DescriptionFileParserLoader();
			if(!parser.load(ckanParser))
				parser.parse();
		}
	}

}
