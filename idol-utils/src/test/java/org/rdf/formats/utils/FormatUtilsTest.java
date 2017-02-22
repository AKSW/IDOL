/**
 * 
 */
package org.rdf.formats.utils;

import org.aksw.idol.utils.FormatsUtils;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author Ciro Baron Neto
 *
 *         Feb 22, 2017
 */
public class FormatUtilsTest {

	/**
	 * Test serialization format extraction using Apache JENA formats
	 */
	@Test
	public void testJENAFormats() {
		FormatsUtils formatUtils = new FormatsUtils();
		
		String url = "http://downloads.url.org/file.ttl";
		Assert.assertEquals("TTL", formatUtils.getJenaFormat(url));

		url = "http://downloads.url.org/file.ttl.gz";
		Assert.assertEquals("TTL", formatUtils.getJenaFormat(url));

		url = "http://downloads.url.org/file.ttl.tar.gz";
		Assert.assertEquals("TTL", formatUtils.getJenaFormat(url));

		url = "http://downloads.url.org/file.ttl.tgz";
		Assert.assertEquals("TTL", formatUtils.getJenaFormat(url));
		
		url = "http://downloads.url.org/file.nt";
		Assert.assertEquals("N-TRIPLES", formatUtils.getJenaFormat(url));

		url = "http://downloads.url.org/file.rdf";
		Assert.assertEquals("RDF/XML", formatUtils.getJenaFormat(url));

		url = "http://downloads.url.org/file.jsonld";
		Assert.assertEquals("JSON-LD", formatUtils.getJenaFormat(url));

	}
	
	
	/**
	 * Test serialization format extraction
	 */
	@Test
	public void testSerialization(){
		
		String url = "http://downloads.url.org/file.ttl";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.TTL, FormatsUtils.getSerializationFormat(url));
		
		url = "http://downloads.url.org/file.ttl.gz";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.TTL, FormatsUtils.getSerializationFormat(url));
		
		url = "http://downloads.url.org/file.ttl.tar";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.TTL, FormatsUtils.getSerializationFormat(url));
		
		url = "http://downloads.url.org/file.ttl.tar.gz";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.TTL, FormatsUtils.getSerializationFormat(url));
		
		url = "http://downloads.url.org/file.n3";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.N3, FormatsUtils.getSerializationFormat(url));

		url = "http://downloads.url.org/file.jsonld";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.JSON_LD, FormatsUtils.getSerializationFormat(url));

		url = "http://downloads.url.org/file.rdf";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.RDF, FormatsUtils.getSerializationFormat(url));

		url = "http://downloads.url.org/file.nt";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.NT, FormatsUtils.getSerializationFormat(url));

		url = "http://downloads.url.org/file.tql";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.TQL, FormatsUtils.getSerializationFormat(url));
		

		url = "http://downloads.url.org/sparql";
		Assert.assertEquals(FormatsUtils.SERIALIZATION_FORMAT.SPARQL, FormatsUtils.getSerializationFormat(url));
		
	}
	
	/**
	 * Test compression format extraction
	 */
	@Test
	public void testCompression(){
		
		String url = "http://downloads.url.org/file.ttl.bz2";
		Assert.assertEquals(FormatsUtils.COMPRESSION_FORMATS.BZ2, FormatsUtils.getCompressionFormat(url));

		url = "http://downloads.url.org/file.ttl.tgz";
		Assert.assertEquals(FormatsUtils.COMPRESSION_FORMATS.TGZ, FormatsUtils.getCompressionFormat(url));

		url = "http://downloads.url.org/file.ttl.tar.gz";
		Assert.assertEquals(FormatsUtils.COMPRESSION_FORMATS.TAR_GZ, FormatsUtils.getCompressionFormat(url));

		url = "http://downloads.url.org/file.ttl.tar";
		Assert.assertEquals(FormatsUtils.COMPRESSION_FORMATS.TAR, FormatsUtils.getCompressionFormat(url));

		url = "http://downloads.url.org/file.ttl.zip";
		Assert.assertEquals(FormatsUtils.COMPRESSION_FORMATS.ZIP, FormatsUtils.getCompressionFormat(url));

		url = "http://downloads.url.org/file.ttl";
		Assert.assertEquals(FormatsUtils.COMPRESSION_FORMATS.NO_COMPRESSION, FormatsUtils.getCompressionFormat(url));
		
	}

}
