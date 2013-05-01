package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;

public class IndexScan extends IndexScanNode {
	IndexScanNode is;
	public IndexScan(IndexScanNode objIndexScan) {

		super(objIndexScan.table,objIndexScan.schema,objIndexScan.condition);
		this.is=objIndexScan;
		
	}

	public List<Datum[]> doIndexScan(){
		List<Datum[]> lsDatum=Sql.lsMapGlobalData.get(table);
		return lsDatum;

	}
	public List findcolumns(){
		//pnode.getColumns();
		ExprTree cond = is.condition;
		List ls=new ArrayList();
		if(cond!=null && !cond.isEmpty()){
			List ls1=new Expression(cond).findColumns();
			Iterator lsit= ls1.iterator();
			while(lsit.hasNext()){
				ls.add(lsit.next());
			}
		}
		return ls;
	}
}
