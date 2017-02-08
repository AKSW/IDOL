/**
 * 
 */
package idol.mongodb;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import idol.exceptions.LODVaderMissingPropertiesException;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 10, 2016
 */
public class DBSuperClassAdapter<T> extends DBSuperClassNew<T> {

	/**
	 * Constructor for Class DBSuperClass2
	 * 
	 * @param collectionName
	 * @param obj
	 */
	public DBSuperClassAdapter(DBObject obj) {
		super(obj);
	}

	public DBSuperClassAdapter() {
		super();
	}

	/**
	 * Return the list of fields of a given class
	 * 
	 * @param c
	 * @return
	 */
	private List<Field> getFields(T t) {
		Class c = t.getClass();
		return Arrays.asList(c.getDeclaredFields());
	}

	public void save(T t) {
		BasicDBObject obj = new BasicDBObject();
		for (Field f : getFields(t)) {
			try {
				obj.put(f.getName(), f.get(t));
			} catch (IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		mongoDBObject = obj;
		try {
			update();
		} catch (LODVaderMissingPropertiesException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

//	public static void main(String[] args) {
//		DistributionModel model = new DistributionModel();
//		DBSuperClassAdapter<DistributionModel> ad = new DBSuperClassAdapter();
//		ad.save(model);
//
//	}

}
