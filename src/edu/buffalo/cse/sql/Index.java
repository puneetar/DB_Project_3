
package edu.buffalo.cse.sql;

import java.io.File;
import java.util.ArrayList;
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


	public static void createIndex(TableFromFile tableFromFile, IndexType type, int keyCols[]) throws Exception
	{
		int numOfkeys = keyCols.length;
		int frames = 100;

		File idxFile = new File("index"+"_"+type+"_"+tableFromFile.getFile());
		BufferManager bm = new BufferManager(frames);
		FileManager fm = new FileManager(bm);

		TestDataStream ds = new TestDataStream(tableFromFile,numOfkeys,keyCols, 0, 0,0, true);
		IndexKeySpec keySpec = new GenericIndexKeySpec(ds.getSchema(), keyCols);

		int indexSize = ds.getRowCount()/10;
		switch(type){
		case HASH:
			HashIndex.create(fm, idxFile, ds, keySpec, indexSize);
			break;
		case ISAM:
			ISAMIndex.create(fm, idxFile, ds, keySpec);
			break;
		default:
			System.out.println("INVALID TYPE::");
		}

		ManagedFile file = fm.open(idxFile);
	}	

	public static List<Datum[]> getFromIndex(TableFromFile tableFromFile, IndexType type, int keyCols[],Datum get[]) throws Exception
	{
		int numOfkeys = keyCols.length;
		int frames = 100;
		File idxFile = new File("index"+"_"+type+"_"+tableFromFile.getFile());

		if(!idxFile.exists())
			createIndex(tableFromFile, type, keyCols);

		BufferManager bm = new BufferManager(frames);
		FileManager fm = new FileManager(bm);

		TestDataStream ds = new TestDataStream(tableFromFile,numOfkeys,keyCols,0,0,0, true);
		IndexKeySpec keySpec = new GenericIndexKeySpec(ds.getSchema(), keyCols);


		if(get != null && idxFile.exists()) {
			ManagedFile file = fm.open(idxFile);
			IndexFile idx = null;
			switch(type){
			case HASH:
				idx = new HashIndex(file, keySpec);
				break;
			case ISAM:
				idx = new ISAMIndex(file, keySpec);
				break;
			}

			System.out.println("Getting: "+Datum.stringOfRow(get));
			return idx.get(get);
		}
		return null;
	}

	public static List<Datum[]> scanFromIndex(TableFromFile tableFromFile, IndexType type, int keyCols[],Datum from[],Datum to[]) throws Exception
	{
		int numOfkeys = keyCols.length;
		int frames = 100;

		File idxFile = new File("index"+"_"+type+"_"+tableFromFile.getFile());

		if(!idxFile.exists())
			createIndex(tableFromFile, type, keyCols);

		BufferManager bm = new BufferManager(frames);
		FileManager fm = new FileManager(bm);

		TestDataStream ds = new TestDataStream(tableFromFile,numOfkeys,keyCols,0,0,0, true);
		IndexKeySpec keySpec = new GenericIndexKeySpec(ds.getSchema(), keyCols);


		if((to != null || from !=null )&& idxFile.exists()) {
			ManagedFile file = fm.open(idxFile);
			IndexFile idx = null;
			switch(type){
			case HASH:
				System.err.println("HASH Index scan >= <= unsupported");
				System.exit(-1);
				break;
			case ISAM:
				idx = new ISAMIndex(file, keySpec);
				break;
			}
			Iterator<Datum[]> scan;
			if(from == null){
				if(to == null){ scan = idx.scan(); }
				else { scan = idx.rangeScanTo(to); }
			} else {
				if(to == null){ scan = idx.rangeScanFrom(from); }
				else { scan = idx.rangeScan(from,to); }
			}
			
			try {
				List <Datum[]> return_list= new ArrayList<Datum[]>();
				while(scan.hasNext()){
					return_list.add(scan.next());
				}
				return return_list;
			} finally {
				try {
					((IndexIterator)scan).close();
				} catch(ClassCastException e) { }
			}		
		}
		return null;
	}


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