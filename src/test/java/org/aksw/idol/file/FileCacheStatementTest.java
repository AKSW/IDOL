package org.aksw.idol.file;

import java.io.IOException;

import org.aksw.idol.BaseConfig;
import org.aksw.idol.properties.Properties;
import org.aksw.idol.services.StatementService;
import org.aksw.idol.uniq.DatasourcesUniqTriples;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired; 
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes= {BaseConfig.class, DatasourcesUniqTriples.class,
		FileCacheStatement.class, StatementService.class})
public class FileCacheStatementTest {

	@Autowired
	FileCacheStatement fileCacheStatement;
	
	@Autowired
	StatementService statementService;
	
	@Autowired
	Properties p;
	
	String subject = "subject";
	String predicate = "predicate";
	String object = "object";

	
	@Test
	public void createStatement() throws IOException {
		System.out.println(p.getIdolproperties().getNrthreads());
		fileCacheStatement.openWriter();
		fileCacheStatement.writeStatement(statementService.createStatement(subject, predicate, object));
		fileCacheStatement.closeWriter();		
	}

}
