/**
 * 
 */
package org.aksw.idol.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.aksw.idol.properties.Properties;
import org.aksw.idol.services.StatementService;
import org.openrdf.model.Statement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
@Service
@Scope("prototype")
public class FileCacheStatement {

	static String FILE_PREFIX = "IDOL_CACHE_UNIQ.";

	static int CACHE_FILE_COUNTER = 0;

	private Gson gson = new Gson();

	private JsonWriter writer;

	private JsonReader reader;

	private BufferedOutputStream out;

	private BufferedInputStream in;

	private StatementService statementService;

	public String fileName;
	
	private Properties properties;

	@Autowired
	public FileCacheStatement(Properties properties, StatementService statementService) {
		fileName = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource().getTmpDir() + "/"
				+ FILE_PREFIX + CACHE_FILE_COUNTER;
		this.statementService = statementService;
		this.properties = properties;
		CACHE_FILE_COUNTER++;
	}
	
	public void createNewFile() {
		fileName = properties.getIdolproperties().getTasks().getCalculateUniqPerDataSource().getTmpDir() + "/"
				+ FILE_PREFIX + CACHE_FILE_COUNTER;
		CACHE_FILE_COUNTER++;
	}
	
	public void removeFile() {
		new File(fileName).delete();
		writer = null;
		reader = null;
	}

	public void openWriter() throws IOException {
		if (writer == null) {
			new File(fileName).delete();
			out = new BufferedOutputStream(new FileOutputStream(new File(fileName)));
			writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
			writer.setIndent("  ");
			writer.beginArray();
		}
	}

	public void openReader() throws IOException {
		if (reader == null) {
			in = new BufferedInputStream(new FileInputStream(new File(fileName)));
			reader = new JsonReader(new InputStreamReader(in, "UTF-8"));
			reader.beginArray();
		}
	}

	public void closeWriter() {
		try {
			writer.endArray();
			writer.close();
			writer = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeReader() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeStatement(Statement st) {
		if (writer == null) {
			try {
				openWriter();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		gson.toJson(new RDFStatementGson(st.getSubject().stringValue(), st.getPredicate().stringValue(),
				st.getObject().stringValue()), RDFStatementGson.class, writer);
	}

	public Statement getStatement() throws IOException {
		RDFStatementGson stmt = gson.fromJson(reader, RDFStatementGson.class);
		return statementService.createStatement(stmt.getSubject(), stmt.getProperty(), stmt.getObject());
	}

	public boolean hasNext() throws IOException {
		return reader.hasNext();
	}

}
