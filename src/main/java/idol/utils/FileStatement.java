/**
 * 
 */
package idol.utils;

import org.openrdf.model.Statement;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
public class FileStatement extends FileList<String> {

	StatementUtils statementUtils = new StatementUtils();
	
	/**
	 * Constructor for Class FileTriple
	 * 
	 * @param path
	 * @param fileName
	 */
	public FileStatement(String path, String fileName) {
		super(path, fileName);
	}
	
	public FileStatement(String fullPath) {
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
