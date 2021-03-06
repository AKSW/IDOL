/**
 * 
 */
package org.aksw.idol.file;

import org.aksw.idol.services.StatementService;
import org.openrdf.model.Statement;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
public class FileStatementCustom extends FileLazyHandler<String> {

	StatementService statementUtils = new StatementService();
	
	/**
	 * Constructor for Class FileTriple
	 * 
	 * @param path
	 * @param fileName
	 */
	public FileStatementCustom(String path, String fileName) {
		super(path, fileName);
	}
	
	public FileStatementCustom(String fullPath) {
		super(fullPath.split("__RAW_")[0],"__RAW_" + fullPath.split("__RAW_") [1]);
	}

	public void writeStatement(Statement st) {
		add("<statement>");
		add(st.getSubject().stringValue());
		add(st.getPredicate().stringValue());
		add(st.getObject().stringValue());
		add("</statement>");
	}
	
	@Override
	public boolean hasNext(){
		return super.hasNext();
	}
	
	public Statement getStatement(){
		// read <statement>
		next();
		
		// tread the triple
		String s = next();
		String p = next();
		String o = next();
		
		String e = next();
		while(!e.equals("</statement>")){
			o = o + System.getProperty("line.separator") + e;
			e = next();
		}
		
		return statementUtils.createStatement(s, p, o);	
	}
}
