package lodVader.spring.REST;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lodVader.application.LODVader;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		// else {
		SpringApplication.run(Application.class, args);

		LODVader mainApp = new LODVader();
		mainApp.Manager();
	}
	
}
