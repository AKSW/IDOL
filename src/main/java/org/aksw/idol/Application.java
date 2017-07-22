package org.aksw.idol;

import javax.annotation.PostConstruct;

import org.aksw.idol.application.Manager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class Application {
	
	@Autowired
	Manager manager;

	public static void main(String[] args) {
		// else {
		SpringApplication.run(Application.class, args);

		Manager mainApp = new Manager();
		mainApp.start();
	}
	
	@PostConstruct
	public void startApp(){
		manager.start();
	}
	
}
