package org.aksw.idol;

import org.aksw.idol.application.LODVader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		// else {
		SpringApplication.run(Application.class, args);

		LODVader mainApp = new LODVader();
		mainApp.Manager();
	}
	
}
