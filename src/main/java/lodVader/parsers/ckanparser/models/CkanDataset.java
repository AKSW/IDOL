/**
 * 
 */
package lodVader.parsers.ckanparser.models;

import java.util.ArrayList;
import java.util.List;

/**
 * CKAN's dataset
 * 
 * @author Ciro Baron Neto
 * 
 *         Oct 26, 2016
 */
public class CkanDataset {

	String id;

	String title;

	String version;

	String licenseUrl;

	List<CkanResource> resources = new ArrayList<>();

	/**
	 * @param resources
	 *            Set the resources value.
	 */
	public void setResources(List<CkanResource> resources) {
		this.resources = resources;
	}

	/**
	 * @return the resources
	 */
	public List<CkanResource> getResources() {
		return resources;
	}

	/**
	 * @param id
	 *            Set the id value.
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title
	 *            Set the title value.
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @param version
	 *            Set the version value.
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	/**
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * @return the licenseUrl
	 */
	public String getLicenseUrl() {
		return licenseUrl;
	}

	/**
	 * @param licenseUrl
	 *            Set the licenseUrl value.
	 */
	public void setLicenseUrl(String licenseUrl) {
		this.licenseUrl = licenseUrl;
	}

}
