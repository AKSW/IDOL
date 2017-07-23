package org.aksw.idol;

import org.aksw.idol.application.Manager;
import org.aksw.idol.properties.Properties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
public class BaseConfig {
	
	@Bean
	public Manager getManager(){
		return new Manager(getProperties());
	}
	
	@Bean
	public Properties getProperties(){
		return new Properties();
	}
	
}
