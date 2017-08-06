package org.aksw.idol;

import org.aksw.idol.application.Manager;
import org.aksw.idol.properties.Properties;
import org.aksw.idol.streaming.IDOLFileStream;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableConfigurationProperties
public class BaseConfig {
	
	@Bean
	public Manager getManager(){
		return new Manager();
	}
	
	@Bean
	public Properties getProperties(){
		return new Properties();
	}
	
//	@Bean
//	public DatasourcesUniqTriples getDatasourcesUniqTriples() {
//		return new DatasourcesUniqTriples();
//	}
	
//	@Profile(value="dryrun")
	@Bean
	public IDOLFileStream getIDOLFileStream(){
		return new IDOLFileStream();
	}
}
