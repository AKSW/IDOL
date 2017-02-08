/**
 * 
 */
package idol.parsers.ckanparser.models;

/**
 * CKAN's resource
 * @author Ciro Baron Neto
 * 
 * Oct 26, 2016
 */
public class CkanResource {
	
	String id;
	
	String url;
	
	String format;
	
	String title;

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * @param id 
	 * Set the id value.
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
	
	/**
	 * @param url 
	 * Set the url value.
	 */
	public void setUrl(String url) {
		this.url = url;
	}
	
	/**
	 * @return the format
	 */
	public String getFormat() {
		return format;
	}
	
	/**
	 * @param format 
	 * Set the format value.
	 */
	public void setFormat(String format) {
		this.format = format;
	}
	
	
	/**
	 * @param title 
	 * Set the title value.
	 */
	public void setTitle(String title) {
		this.title = title;
	}
	
	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}
	
	
	
}
