/**
 * 
 */
package org.aksw.idol.services;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.springframework.stereotype.Service;

/**
 * @author Ciro Baron Neto
 * 
 *         Nov 13, 2016
 */
@Service
public class StatementService {

	public Statement createStatement(String subject, String predicate, String object) {

		return new Statement() {

			@Override
			public Resource getSubject() {
				// TODO Auto-generated method stub
				return new Resource() {

					@Override
					public String stringValue() {
						return subject;
					}
				};
			}

			@Override
			public URI getPredicate() {
				return new URI() {

					@Override
					public String stringValue() {
						return predicate;
					}

					@Override
					public String getNamespace() {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public String getLocalName() {
						// TODO Auto-generated method stub
						return null;
					}
				};
			}

			@Override
			public Value getObject() {
				return new Value() {

					@Override
					public String stringValue() {
						return object;
					}
				};
			}

			@Override
			public Resource getContext() {
				// TODO Auto-generated method stub
				return null;
			}

		};

	}
}
