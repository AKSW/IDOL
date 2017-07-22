package org.aksw.idol;

import org.aksw.idol.application.Manager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BaseConfig {
	
	@Bean
	public Manager getManager(){
		return new Manager();
	}
}
