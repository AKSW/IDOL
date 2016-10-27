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

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.ckanparser.CkanCatalogDB;
import lodVader.mongodb.collections.ckanparser.CkanDatasetDB;
import lodVader.mongodb.collections.ckanparser.CkanResourceDB;
import lodVader.mongodb.collections.ckanparser.adapters.CkanCatalogDBAdapter;
import lodVader.mongodb.collections.ckanparser.adapters.CkanDatasetDBAdapter;
import lodVader.parsers.ckanparser.CkanDatasetList;
import lodVader.parsers.ckanparser.CkanParser;
import lodVader.parsers.ckanparser.models.CkanDataset;
import lodVader.parsers.ckanparser.models.CkanResource;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 1, 2016
 */
public class CKANRepositoriesLoader {

	final static Logger logger = LoggerFactory.getLogger(CKANRepositoriesLoader.class);

	// number of concurrent request to be made for each repository
	final int numberOfConcurrentRequests = 1;

	// number of repositories to be analyzed concurrently
	final int numberOfConcurrentRepositories = 3;
	// static ArrayList<String> ckanRepositories = new
	// ArrayList<>(Arrays.asList("http://africaopendata.org/"));
	// static ArrayList<String> ckanRepositories = new
	// ArrayList<>(Arrays.asList("http://drdsi.jrc.ec.europa.eu"));
	// "http://go.arenysdemunt.cat/ca/dataset",
	// "http://daten.berlin.de",
	// "http://cities.opendatahub.gr",
	// "http://dados.ima.sp.gov.br",
	// "http://ckan.sabae.jrrk.org",
	// "http://dataforjapan.org",
	// "http://data.suwon.go.kr",
	// "http://www.data.go.jp",
	// "http://datacatalogs.org/",
	// "http://datagm.org.uk",
	// "http://www.dati.gov.it",
	// "https://gong.io",
	// "http://data.linz.gv.at/",
	// "http://geothermaldata.org",
	// "http://www.offene-daten.me",
	// "http://www.opendatahub.it/",
	// "https://dati.lazio.it/catalog/it",
	// "http://publicdata.eu",
	// "http://data.cityofsantacruz.com/",
	// "http://udct-data.aigid.jp",
	// "http://data.yokohamaopendata.jp"

	public static ArrayList<String> ckanRepositories = new ArrayList<>(Arrays.asList(
			"https://africaopendata.org/",
			"http://dados.al.gov.br", "https://open.alberta.ca", "http://data.amsterdam.nl",
			"http://annuario.comune.fi.it", "http://opendata.aragon.es/", "http://apicatalogo.santander.es/",
			"https://data.barrowbc.gov.uk", "https://catalogue.data.gov.bc.ca", "http://bermuda.io/",
			"http://data.buenosaires.gob.ar/", "https://catalogodatos.gub.uy/", "https://www.datos.gov.py",
			"http://data.ottawa.ca/", "http://data.surrey.ca/", "http://data.zagreb.hr", "http://www.civicdata.io",
			"https://datahub.cmap.illinois.gov", "http://opendata.cmt.es/", "http://datos.codeandomexico.org/",
			"http://data.kk.dk/", "http://dados.recife.pe.gov.br/", "http://dados.gov.br", "http://dados.rs.gov.br",
			"http://drdsi.jrc.ec.europa.eu", "http://dartportal.leeds.ac.uk/", "http://datar.noip.me",
			"http://data.bris.ac.uk/data", "http://open.canada.ca/data/en", "http://catalog.data.gov",
			"http://data.gov.au", "https://data.gov.ie", "http://data.gov.ro/", "https://data.gov.sk",
			"https://data.gov.uk", "http://ckan.data.ktn.gv.at/", "https://data.gov.au",
			"https://data.overheid.nl/data/", "http://data.rio.rj.gov.br/", "http://data.salzburgerland.com",
			"https://www.data.vic.gov.au/data/", "http://catalogue.beta.data.wa.gov.au", "http://datamx.io",
			"http://datapoa.com.br/", "http://datosabiertos.malaga.eu/", "http://catalogo.datos.gob.mx",
			"http://datosabiertos.ec", "https://data.denvergov.org/", "http://ecaidata.org", "https://ecds.se",
			"http://data.edostate.gov.ng/", "https://edx.netl.doe.gov", "https://opendata.riik.ee",
			"https://etsin.avointiede.fi", "http://data.europa.eu/euodp/data/", "http://www.europeandataportal.eu/data",
			"http://dados.fortaleza.ce.gov.br", "http://ckan.gsi.go.jp", "https://data.glasgow.gov.uk/",
			"https://www.govdata.de/ckan", "http://data.grcity.us", "http://ckan.data.graz.gv.at/",
			"http://opendatahub.gr", "http://www.hri.fi", "http://data.ohouston.org/",
			"https://datastore.landcareresearch.co.nz/", "http://www.opendata.provincia.roma.it/", "http://data.go.id",
			"http://130.179.67.140", "https://datastore.landcareresearch.co.nz", "http://data.lexingtonky.gov/",
			"https://gisdata.mn.gov/", "http://donnees.ville.montreal.qc.ca/", "http://data.nantou.gov.tw",
			"https://data.noaa.gov/", "http://www.nosdonnees.fr/", "https://offenedaten.de/", "http://www.odaa.dk/",
			"http://www.odaa.dk/", "http://data.gov.bf", "http://opendatacanarias.es/datos", "http://dati.toscana.it/",
			"http://opendatagortynia.gr/", "http://catalog.data.ug", "http://datameti.go.jp/data",
			"http://opendata.aachen.de", "http://suche.transparenz.hamburg.de/", "http://data.gov.hr",
			"http://dati.trentino.it/", "http://data.openva.com/", "http://portal.openbelgium.be",
			"http://data.opencolorado.org/", "http://opendata.caceres.es/", "http://opendata.lisra.jp",
			"http://opendata.awt.be/", "https://opendata.gov.je", "http://www.opendata-hro.de/", "http://opendata.hu/",
			"http://data.wu.ac.at", "https://www.opendatabc.ca", "http://opendatadc.org/",
			"http://data.opendataportal.at", "http://opendatareno.org", "http://dati.openexpo2015.it/catalog",
			"https://hubofdata.ru/", "http://ckan.openumea.se/", "https://opingogn.is/",
			"http://dados.prefeitura.sp.gov.br", "http://www.datos.misiones.gov.ar", "http://datospublicos.gob.ar/",
			"http://opendata.comune.bari.it/", "https://data.qld.gov.au", "http://catalogue.datalocale.fr",
			"https://www.daten.rlp.de/", "http://rotterdamopendata.nl/", "http://donnees.ville.sherbrooke.qc.ca",
			"http://data.sa.gov.au/data", "https://opendata.swiss/", "http://taijiang.tw", "http://data.tainan.gov.tw",
			"http://dadosabertos.senado.gov.br/", "https://datahub.io/", "https://iatiregistry.org",
			"http://data.london.gov.uk", "http://data-gov-ua.org", "http://oppnadata.se"));

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

		System.out.println("All CKAN repositories loaded.");

	}

	/**
	 * Load many CKAN repositories concurrently
	 * 
	 * @author Ciro Baron Neto
	 * 
	 *         Oct 1, 2016
	 */
	class HttpRepositoryRequestThread implements Runnable {

		String ckanCatalog;


		// CkanClient client;

		public HttpRepositoryRequestThread(String ckanCatalog) {
			this.ckanCatalog = ckanCatalog;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			CkanParser parser = new CkanParser(ckanCatalog);
			
			CkanCatalogDB catalogDB = new CkanCatalogDBAdapter(parser.getCkanCatalog());
	
			try {
				catalogDB.update();
			} catch (LODVaderMissingPropertiesException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			CkanDatasetList list = parser.getDatasetList();
			while(list.hasNext()){
				CkanDataset dataset = list.next();
				CkanDatasetDB datasetDB = new CkanDatasetDBAdapter(dataset, ckanCatalog);
				
				try {
					datasetDB.update();
				} catch (LODVaderMissingPropertiesException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}
	}

}
