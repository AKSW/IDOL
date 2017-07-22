/**
 * 
 */
package org.aksw.idol.parsers.ckanparser.models;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 26, 2016
 */
public class CkanDatasetAdapterV3 extends CkanDataset {

	/**
	 * Constructor for Class CkanDatasetAdapterV3
	 */
	public CkanDatasetAdapterV3(JSONObject object) {

		try {
			setId(object.get("id").toString());
		} catch (Exception e) {
			setId(null);
		}

		try {
			setLicenseUrl(object.get("license_url").toString());

		} catch (Exception e) {
			setLicenseUrl((null));
		}

		try {
			setTitle(object.get("title").toString());
		} catch (Exception e) {
			setTitle(null);
		}

		try {
			setVersion(object.get("version").toString());
		} catch (Exception e) {
			setVersion(null);
		}

		try {
			JSONArray jsonResources = object.getJSONArray("resources");

			List<CkanResource> resources = new ArrayList<>();
			jsonResources.forEach((resource) -> {
				
				CkanResource r = new CkanResource();
				JSONObject o = (JSONObject) resource;
				
				try {
					r.setFormat(o.get("format").toString());
				} catch (Exception e) {
					r.setFormat(null);
				}

				try {
					r.setUrl(o.get("url").toString());
				} catch (Exception e) {
					r.setUrl(null);
				}

				try {
					r.setId(o.get("id").toString());
				} catch (Exception e) {
					r.setId(null);
				}

				try {
					r.setTitle(o.get("name").toString());
				} catch (Exception e) {
					r.setTitle(null);
				}

				resources.add(r);

			});

			setResources(resources);

		} catch (Exception e) {
		}

	}

}
