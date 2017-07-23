package org.aksw.idol;

import javax.annotation.PostConstruct;

import org.aksw.idol.application.Manager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	@Autowired
	Manager manager;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@PostConstruct
	public void startApp() {
		manager.start();
	}

}
