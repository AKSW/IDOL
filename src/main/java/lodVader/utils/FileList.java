/**
 * 
 */
package lodVader.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Iterator;

/**
 * This class uses a file to store and manage a list
 * 
 * @author Ciro Baron Neto
 * 
 *         Nov 11, 2016
 */
public class FileList<T> extends AbstractList<T> implements Iterator<T> {

	private String path = null;

	private String fileName = null;

	private BufferedWriter bw = null;

	private BufferedReader br = null;

	private int size = 0;

	private T line;
	
	/**
	 * @return the path
	 */
	public String getFullPath() {
		return path + fileName;
	}

	/**
	 * Constructor for Class FileList
	 * 
	 * @param path
	 *            the path used to store the file
	 */
	public FileList(String path, String fileName) {

		if (!path.endsWith("/"))
			path = path + "/";

		this.path = path;
		this.fileName = fileName;
		try {
			new File(getFullPath()).createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Collection#add(java.lang.Object)
	 */
	@Override
	public boolean add(T e) {
		if (addElement(e)) {
			size++;
			return true;
		}
		return false;
	}

	private boolean addElement(T e) {

		if (bw == null) {
			try {
				bw = new BufferedWriter(new FileWriter(new File(path + fileName)));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		try {
			bw.write(e.toString());
			bw.newLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Collection#clear()
	 */
	@Override
	public void clear() {
		closeBuffers();
		File f = new File(path + fileName);
		f.delete();
		size = 0;
	}

	private void closeBuffers() {
		closeBufferedReader();
		closeBufferedWriter();
	}

	private void closeBufferedReader() {
		if (br != null)
			try {
				br.close();
				br = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	private void closeBufferedWriter() {
		if (bw != null)
			try {
				bw.close();
				bw = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	public void close() {
		closeBufferedReader();
		closeBufferedWriter();
	}

	private void openBufferedWriter() {
		if (bw == null) {
			try {
				bw = new BufferedWriter(new FileWriter(new File(path + fileName)), 1024*8*32);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	private void openBufferedReader() {
		if (br == null) {
			try {
				br = new BufferedReader(new FileReader(new File(path + fileName)), 1024*8*32);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			updateLine();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Collection#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		if (size == 0)
			return true;
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Collection#size()
	 */
	@Override
	public int size() {
		return size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.AbstractList#get(int)
	 */
	@Override
	public T get(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		openBufferedReader();
		if (line != null)
			return true;
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		closeBufferedWriter();
		openBufferedReader();

		T s = line;
		updateLine();

		return s;
	}

	private void updateLine() {
		try {
			line = (T) br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @return the br
	 */
	public BufferedReader getBr() {
		return br;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.AbstractList#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
		closeBufferedReader();
		openBufferedReader();
		return super.iterator();
	}

//	public static void main(String[] args) {
//		FileList<Integer> f = new FileList<>("/tmp", "oiii");
//
//		f.add(234);
//		f.add(234444);
//		f.add(2324);
//		f.add(234);
//
//		f.close();
//		
//
//
//		for (Integer s : f) {
//			System.out.println(f.next());
//		}
//		for (Integer s : f) {
//			System.out.println(f.next());
//		}
//		System.out.println(f.size());
//		f.clear();
//
//	}

}
