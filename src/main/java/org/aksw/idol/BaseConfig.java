package org.aksw.idol;

import java.util.concurrent.Executor;

import org.aksw.idol.application.Manager;
import org.aksw.idol.properties.Properties;
import org.aksw.idol.streaming.IDOLFileStreamImpl;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableConfigurationProperties
@EnableAsync
public class BaseConfig {
	
	@Bean
	public Manager getManager(){
		return new Manager();
	}
	
	@Bean
	public Properties getProperties(){
		return new Properties();
	}
	
    @Bean
    public Executor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1);
        executor.setThreadNamePrefix("Manager-");
        executor.initialize();
        return executor;
    }
	
//	@Bean
//	public DatasourcesUniqTriples getDatasourcesUniqTriples() {
//		return new DatasourcesUniqTriples();
//	}
	
//	@Profile(value="dryrun")
	@Bean
	public IDOLFileStreamImpl getIDOLFileStream(){
		return new IDOLFileStreamImpl();
	}
}
