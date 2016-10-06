package lodVader.spring.REST;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lodVader.application.LODVader;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		// if (LODVaderProperties.EVALUATE_LINKS) {
		// LinksCLOD l = new LinksCLOD();
		//// l.checkCohesion();
		//// l.checkCohesion();
		//// LOV2 lov = new LOV2();
		//// try {
		//// lov.loadLOVVocabularies();
		//// } catch (Exception e) {
		//// // TODO Auto-generated catch block
		//// e.printStackTrace();
		//// }
		//
		// TestLinks t = new TestLinks();
		//
		//
		// }
		//
		// else {
		SpringApplication.run(Application.class, args);

		LODVader mainApp = new LODVader();
		mainApp.Manager();
	}
	
}
