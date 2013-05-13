
package edu.buffalo.cse.sql;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.index.GenericIndexKeySpec;
import edu.buffalo.cse.sql.index.IndexFile;
import edu.buffalo.cse.sql.index.IndexIterator;
import edu.buffalo.cse.sql.index.ISAMIndex;
import edu.buffalo.cse.sql.index.HashIndex;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.test.TestDataStream;

public class Index {
	public enum IndexType { HASH, ISAM };

	public static Datum[] parseRow(String row){
		String[] fields = row.split(", *");
		Datum[] ret = new Datum[fields.length];
		for(int i = 0; i < ret.length; i++){
			ret[i] = new Datum.Int(Integer.parseInt(fields[i]));
		}
		return ret;
	}

	public static void createIndex(TableFromFile tableFromFile, List<ExprTree> ls, IndexType type, int keys[],int values,
			boolean toScan,	Datum get[], Datum from[], Datum to[]) throws Exception
	{
		//IndexType type = IndexType.HASH;
		int numOfkeys = keys.length;
		
		int rows = 100;
		int frames = 1024;
		int keychaos = 2;
		int indexSize = rows/10;

		File idxFile = new File("index.dat");

		BufferManager bm = new BufferManager(frames);
		FileManager fm = new FileManager(bm);

		TestDataStream ds = new TestDataStream(tableFromFile,numOfkeys, values, rows, keychaos, true);
		IndexKeySpec keySpec = new GenericIndexKeySpec(ds.getSchema(), numOfkeys);

//		if(toScan){
//			ManagedFile file = fm.open(idxFile);
//			IndexFile idx = null;
//			switch(type){
//			case HASH:
//				System.err.println("HASH Index scan validation unsupported");
//				System.exit(-1);
//				break;
//			case ISAM:
//				idx = new ISAMIndex(file, keySpec);
//				break;
//			}
//			Iterator<Datum[]> scan;
//			if(from == null){
//				if(to == null){ scan = idx.scan(); }
//				else { scan = idx.rangeScanTo(to); }
//			} else {
//				if(to == null){ scan = idx.rangeScanFrom(from); }
//				else { scan = idx.rangeScan(from,to); }
//			}
//			try {
//				if(ds.validate(scan, from, to)){
//					System.out.println("Test Successful!");
//					System.exit(0);
//				} else {
//					System.out.println("Test Failed!");
//					System.exit(-1);
//				}
//			} finally {
//				try {
//					((IndexIterator)scan).close();
//				} catch(ClassCastException e) { }
//			}
//		} else if(get != null) {
//			ManagedFile file = fm.open(idxFile);
//			IndexFile idx = null;
//			switch(type){
//			case HASH:
//				idx = new HashIndex(file, keySpec);
//				break;
//			case ISAM:
//				idx = new ISAMIndex(file, keySpec);
//				break;
//			}
//
//			System.out.println("Getting: "+Datum.stringOfRow(get));
//			// TODO index.java get = idx.get(get);
//			System.out.println("Got: "+((get==null)?"Nothing"
//					:Datum.stringOfRow(get)));
//
//		} else {
//			switch(type){
//			case HASH:
//				HashIndex.create(fm, idxFile, ds, keySpec, indexSize);
//				break;
//			case ISAM:
//				ISAMIndex.create(fm, idxFile, ds, keySpec);
//				break;
//			}
//		}
//
//		ManagedFile file = fm.open(idxFile);
//
	}
}