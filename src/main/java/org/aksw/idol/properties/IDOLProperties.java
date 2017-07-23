package org.aksw.idol.properties;

public class IDOLProperties {
	
	public int nrthreads;
	
	public static enum Streaming{
		
		LOCAL("local"),
		INTERNET("internet");
		
		String type;
		
		private Streaming(String type) {
			this.type = type;
		}
	}
	
	Streaming streaming;
	
	
	public TaskProperties tasks = new TaskProperties();
	
	public ParseProperties parse = new ParseProperties();

	public int getNrthreads() {
		return nrthreads;
	}

	public void setNrthreads(int nrthreads) {
		this.nrthreads = nrthreads;
	}

	public Streaming getStreaming() {
		return streaming;
	}

	public void setStreaming(Streaming streaming) {
		this.streaming = streaming;
	}

	public TaskProperties getTasks() {
		return tasks;
	}

	public void setTasks(TaskProperties tasks) {
		this.tasks = tasks;
	}

	public ParseProperties getParse() {
		return parse;
	}

	public void setParse(ParseProperties parse) {
		this.parse = parse;
	}
	
}
