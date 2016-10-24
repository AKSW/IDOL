package lodVader.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.exceptions.LODVaderFormatNotAcceptedException;
import lodVader.loader.LODVaderProperties;

public class FileUtils {

	final static Logger logger = LoggerFactory.getLogger(FileUtils.class);

	public static void checkIfFolderExists() {

		// check if folders needed exists
		File f = new File(LODVaderProperties.BASE_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.FILTER_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.SUBJECT_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.TMP_FOLDER);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.OBJECT_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.FILE_URL_PATH);
		if (!f.exists())
			f.mkdirs();

		f = new File(LODVaderProperties.DUMP_PATH);
		if (!f.exists())
			f.mkdirs();
	}

	// TODO make this method more precise
	public static boolean acceptedFormats(String fileName) throws LODVaderFormatNotAcceptedException {

		if (fileName.contains(".ttl"))
			return true;
		else if (fileName.contains(".nt"))
			return true;
		else if (fileName.contains(".rdf"))
			return true;
		else if (fileName.contains(".zip"))
			return true;
		else if (fileName.contains(".bzip"))
			return true;
		else if (fileName.contains(".tgz"))
			return true;
		else if (fileName.contains(".gz"))
			return true;
		else {
			// throw new
			// DynamicLODFormatNotAcceptedException("File format not accepted: "
			// + fileName);
			return true;
		}
	}

	public static String stringToHash(String str) {
		String original = str;
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(original.getBytes());
			byte[] digest = md.digest();
			StringBuffer sb = new StringBuffer();
			for (byte b : digest) {
				sb.append(String.format("%02x", b & 0xff));
			}

			logger.debug("Creating hash name for:" + original);
			logger.debug("digested(hex):" + sb.toString());
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getASCIIFormat(String str) {
		return str.replaceAll("[^A-Za-z0-9]", "");
	}

	/**
	 * Get file name based on the URL
	 * 
	 * @param accessURL
	 * @param httpDisposition
	 * @return
	 */
	public String getFileName(String accessURL, String httpDisposition) {
		String fileName = null;

		if (httpDisposition != null) {
			int index = httpDisposition.indexOf("filename=");
			if (index > 0) {
				fileName = httpDisposition.substring(index + 10, httpDisposition.length() - 1);
			}
		} else {

			// extracts file name from URL
			fileName = accessURL.substring(accessURL.lastIndexOf("/") + 1, accessURL.length());
		}

		return fileName;
	}

	public void sortFile(String file) {

		try {
			ExternalSortLocal.sort(new File(file), new File(file + ".sorted"));
			removeFile(file);
			Files.move(Paths.get(file + ".sorted"), Paths.get(file));

			logger.info("File " + file + " sorted.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void removeFile(String file) {
		new File(file).delete();
	}

	//
	
	static BufferedWriter w;
//	public static void main(String[] args) {
//		// new FileUtils().compareTwoFiles("/home/ciro/lodvaderdata/tmp/oi2/1",
//		// "/home/ciro/lodvaderdata/tmp/oi2/2");
//
//		try {
//			w= new BufferedWriter(new FileWriter(new File("/home/ciro/lodvaderdata/tmp/exp")));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		FileUtils aa = new FileUtils();
//		int filterSize = 200000;
//		int amountOfElements = 200000;
//
//		for(int i=10000; i<=200000; i = i + 1000)
//			aa.runExp(filterSize, i);
//		
//		try {
//			w.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
////		System.out.println(((double) (1 + (int)(Math.random() * 150))/10000) * (1 + (int)(Math.random() * 130))/100) ;
////		System.out.println( (1 + (int)(Math.random() * 130))/100);
//	}

//	public void runExp(int filterSize, int amountOfElements) {
//		FileUtils aa = new FileUtils();
//
//		int experiments = 5;
//		
//		for (int i = 0; i < experiments; i++) {
//			BloomFilterI bf1 = BloomFilterFactory.newBloomFilter();
//			bf1.create(filterSize, 0.0000001);
//			BloomFilterI bf2 = BloomFilterFactory.newBloomFilter();
//			bf2.create(filterSize, 0.0000001);
//
//			for (String l : aa.makeDataet(amountOfElements)) {
////				for (String l : aa.makeDataet(10000)) {
//				bf1.add(l);
//			}
////			System.out.println(((BloomFilter<String>) bf1.getImplementation()).getEstimatedPopulation());
//
//			for (String l : aa.makeDataet(amountOfElements)) {
//				bf2.add(l);
//			}
//
////			System.out.println(((BloomFilter<String>) bf1.getImplementation()).getEstimatedPopulation());
//
//			// System.out.println(((BloomFilter<String>)
//			// bf2.getImplementation()).getEstimatedPopulation());
//			//
//			// System.out.println(((BloomFilter<String>)
//			// bf2.getImplementation()).intersect(((BloomFilter<String>)
//			// bf1.getImplementation())));
//			//
//			// System.out.println(((BloomFilter<String>)
////			 bf2.getImplementation()).getEstimatedPopulation());
//			Double intersectValue = bf2.intersection(bf1);
////			System.out.println((amountOfElements + i) + " "+((1 - intersectValue / amountOfElements) -(((double) (1 + (int)(Math.random() * 150))/10000) * (1 + (int)(Math.random() * 130))/100)));
////			System.out.print(" "+((1 - intersectValue / amountOfElements) - (double) (1 + (int)(Math.random() * 150))/10000));
//
//			try {
//				w.write((amountOfElements + i) + " "+((1 - intersectValue / amountOfElements) -(((double) (1 + (int)(Math.random() * 150))/10000) * (1 + (int)(Math.random() * 130))/100)) + "\n");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//
//	}

	public Set<String> makeDataet(int size) {
		Set<String> list = new HashSet<String>();
		while (list.size() < size)
			list.add(gerador3());

		return list;
	}

	public String gerador1() {
		return new BigInteger(130, new Random()).toString(32);
	}

	public String gerador2() {
		return new BigInteger(1120, new Random()).toString(342);

	}

	public String gerador3() {
		return new BigInteger(100, new Random()).toString(32);

	}
	//
	// public void compareTwoFiles(String file1, String file2) {
	//
	// HashSet<String> s1 = new HashSet<>();
	// HashSet<String> s2 = new HashSet<>();
	//
	// try {
	// BufferedReader b1 = new BufferedReader(new FileReader(new File(file1)));
	// BufferedReader b2 = new BufferedReader(new FileReader(new File(file2)));
	//
	// String line;
	//
	// BloomFilterI bf1 = BloomFilterFactory.newBloomFilter();
	// bf1.create(2000000, 0.0000001);
	// BloomFilterI bf2 = BloomFilterFactory.newBloomFilter();
	// bf2.create(2000000, 0.0000001);
	//
	// while ((line = b1.readLine()) != null) {
	// s1.add(line);
	// }
	// for (String l : s1)
	// bf1.add(l);
	//
	// while ((line = b2.readLine()) != null) {
	// s2.add(line);
	// }
	// for (String l : s2)
	// bf2.add(l);
	//
	// // new
	// // BucketDB(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES).saveBF(bf1,
	// // "ola", 0, "", "");
	// // bf1 = new
	// //
	// BucketDBHelper().getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES,
	// // Arrays.asList("ola")).get("ola").iterator().next();
	//
	// // ByteArrayOutputStream out1 = new ByteArrayOutputStream();
	// //
	// // bf1.writeTo(out1);
	// //
	// // final Path destination = Paths.get("/tmp/oi1");
	// // try (
	// // final InputStream in = new BufferedInputStream(new
	// // ByteArrayInputStream(out1.toByteArray()));
	// // ) {
	// // Files.copy(in, destination);
	// // }
	// // -----------
	// // bf1.writeTo(new FileOutputStream(new File("/tmp/oi1")));
	// // bf2.writeTo(new FileOutputStream(new File("/tmp/oi2")));
	// // ----------------
	//
	// // bf1 = null;
	// // bf2 = null;
	//
	// // bf1.readFrom(new FileInputStream(new File("/tmp/oi1")));
	// // bf2.readFrom(new FileInputStream(new File("/tmp/oi2")));
	//
	// // 58052160b5c0f678ea124ee1 5805215fb5c0f678ea124ce9
	// // SUBSET_HASH_SET_DETECTOR 1
	// // 58052160b5c0f678ea124ee1 5805215fb5c0f678ea124ce9
	// // SUBSET_BLOOM_FILTER_DETECTOR 13
	//
	// //
	// bf1 = new BucketDBHelper()
	// .getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES,
	// Arrays.asList("58052160b5c0f678ea124ee1"))
	// .get("58052160b5c0f678ea124ee1").iterator().next();
	// bf2 = new BucketDBHelper()
	// .getDistributionFilters(BucketDB.COLLECTIONS.BLOOM_FILTER_TRIPLES,
	// Arrays.asList("5805215fb5c0f678ea124ce9"))
	// .get("5805215fb5c0f678ea124ce9").iterator().next();
	//
	// System.out.println(s1.size());
	// System.out.println(s2.size());
	// System.out.println(((BloomFilter<String>)
	// bf1.getImplementation()).getEstimatedPopulation());
	// System.out.println(((BloomFilter<String>)
	// bf2.getImplementation()).getEstimatedPopulation());
	//
	// System.out.println(s1.retainAll(s2));
	// System.out.println(s1.size());
	// System.out.println(bf2.intersection(bf1));
	// b1.close();
	// b2.close();
	//
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }

}
