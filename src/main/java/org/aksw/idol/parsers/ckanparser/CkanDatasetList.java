/**
 * 
 */
package org.aksw.idol.parsers.ckanparser;

import java.util.Iterator;
import java.util.List;

import org.aksw.idol.parsers.ckanparser.models.CkanDataset;

/**
 * @author Ciro Baron Neto
 * 
 *         Oct 26, 2016
 */
public class CkanDatasetList implements Iterator<CkanDataset> {

	CkanParserInterface parser;
	
	List<String> datasetIds;
	
	/**
	 * Constructor for Class CkanDatasetList 
	 */
	public CkanDatasetList(CkanParser parser) {
		this.parser = parser;
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		if(datasetIds == null)
			datasetIds = parser.fetchDatasetIds();
		
		if (parser.fetchDatasetIds().isEmpty())
			return false;
		else
			return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#next()
	 */
	@Override
	public CkanDataset next() {
		
		return parser.fetchDataset(datasetIds.remove(0));
	}

}
