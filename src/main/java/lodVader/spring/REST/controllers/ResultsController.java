/**
 * 
 */
package lodVader.spring.REST.controllers;

import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.queries.GeneralQueriesHelper;
import lodVader.plugins.intersection.LODVaderIntersectionPlugin;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 8, 2016
 */

@RestController
public class ResultsController {

	final static Logger logger = LoggerFactory.getLogger(ResultsController.class);

	/**
	 * Return a distribution
	 * 
	 * @param id
	 * @return the distribution
	 */
	@RequestMapping(value = "/results/bfPrecision", method = RequestMethod.GET)
	public String dataset(@PathVariable String id) {
		StringBuilder s = new StringBuilder();

		return s.toString();

	}

//	public static void main(String[] args) {
//		// List<DBObject> list = new
//		// GeneralQueriesHelper().getObjects("PLUGIN_INTERSECTION_PLUGIN", new
//		// BasicDBObject(), null,
//		// LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString());
//		// for(DBObject o : list){
//		// System.out.println(o.get(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION)+
//		// " "+ o.get(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION) +" "+
//		// o.get(LODVaderIntersectionPlugin.IMPLEMENTATION));
//		// }
//
//		Object[] list = new GeneralQueriesHelper()
//				.getObjects("PLUGIN_INTERSECTION_PLUGIN", new BasicDBObject(), null,
//						LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString())
//				.stream().sorted(new Comparator<DBObject>() {
//					/*
//					 * (non-Javadoc)
//					 * 
//					 * @see java.util.Comparator#compare(java.lang.Object,
//					 * java.lang.Object)
//					 */
//					@Override
//					public int compare(DBObject o1, DBObject o2) {
//						if (o1.get(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString()).toString().compareTo(
//								o2.get(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString()).toString()) > 0) {
//							return 1;
//						} else if (o1.get(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString()).toString()
//								.compareTo(o2.get(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION.toString())
//										.toString()) < 0) {
//							return -1;
//						} else {
//
//							if (o1.get(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION.toString()).toString().compareTo(
//									o2.get(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION.toString()).toString()) > 0) {
//								return 1;
//							} else if (o1.get(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION.toString()).toString()
//									.compareTo(o2.get(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION.toString())
//											.toString()) < 0) {
//								return -1;
//							}
//						}
//
//						return 0;
//					}
//				}).toArray();
//		
//		for (Object ob : list) {
//			DBObject o = (DBObject) ob;
//			System.out.println(o.get(LODVaderIntersectionPlugin.SOURCE_DISTRIBUTION) + " "
//					+ o.get(LODVaderIntersectionPlugin.TARGET_DISTRIBUTION) + " "
//					+ o.get(LODVaderIntersectionPlugin.IMPLEMENTATION) + " " +  o.get(LODVaderIntersectionPlugin.VALUE));
//		}
//
//	}

}
