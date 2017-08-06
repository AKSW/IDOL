package org.aksw.idol.file;

public class RDFStatementGson {
		
	public RDFStatementGson(String subject, String property, String object) {
		super();
		this.subject = subject;
		this.property = property;
		this.object = object;
	}

	String subject;
	
	String property;
	
	String object;

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getProperty() {
		return property;
	}

	public void setProperty(String property) {
		this.property = property;
	}

	public String getObject() {
		return object;
	}

	public void setObject(String object) {
		this.object = object;
	}
	
	

}
