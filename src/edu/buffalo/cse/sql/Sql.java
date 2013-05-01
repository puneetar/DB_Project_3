/* Generated By:JavaCC: Do not edit this line. Sql.java */
package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.*;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Column;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.Bool;
import edu.buffalo.cse.sql.data.Datum.Flt;
import edu.buffalo.cse.sql.data.Datum.Str;

import edu.buffalo.cse.sql.optimizer.PlanRewrite;
import edu.buffalo.cse.sql.optimizer.PushDownSelects;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.util.*;
public class Sql {
	public static List<List<Datum[]>> lsGlobalData = new ArrayList<List<Datum[]>>();
	public static HashMap<String, List<Datum[]>> lsMapGlobalData= new HashMap<String, List<Datum[]>>();
	public static HashMap<String,List<String>> hmp_tables_col_used= null;
	public static boolean flag_hmp_tables_col_used=false;
	private static int flag_TPCH=0;
	public static HashMap<String,String> tablemap= new HashMap<String,String>();

	public static void main( String[] args )
	{

		try {
			List<List<Datum[]>> result=Sql.execFile(new File(args[0]));

			TableBuilder output = new TableBuilder();
			//			    for(Schema.Column c : querySchema){
			//			      output.newCell(c.getName());
			//			      cols++;
			//			    }
			Iterator<Datum[]> resultIterator=result.iterator().next().iterator();

			output.addDividerLine();
			while(resultIterator.hasNext()){
				Datum[] row = resultIterator.next();
				output.newRow();
				for(Datum d : row){
					output.newCell(d.toString());
				}
			}

			System.out.println(output.toString());
			//print result;

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (SqlException e) {

			e.printStackTrace();
		} catch (ParseException e) {

			e.printStackTrace();
		}
	}

	public static List<Datum[]> execQuery(Map<String, Schema.TableFromFile> tables,PlanNode q)throws SqlException
	{	PushDownSelects objPush= new PushDownSelects(true);
	PlanNode newq=objPush.rewrite(q);  
	q=newq;
	globalData(tables,q);
	
	System.out.println("GLOBAL data made");
	List<Datum[]> f1=Utility.switchNodes(q);


	TableBuilder output = new TableBuilder();
	
	List<Schema.Var> list=q.getSchemaVars();
	Iterator<Schema.Var> it=list.iterator();
	while(it.hasNext()){
		Schema.Var sc=it.next();
	     output.newCell(sc.name);
		//System.out.println(sc.name+" : "+sc.rangeVariable);
	}


	//	    for(Schema.Column c : querySchema){
	//	      output.newCell(c.getName());
	//	      cols++;
	//	    }
	Iterator<Datum[]> resultIterator=f1.iterator();

	output.addDividerLine();
	while(resultIterator.hasNext()){
		Datum[] row = resultIterator.next();
		output.newRow();
		for(Datum d : row){
			output.newCell(d.toString());
		}
	}

	System.out.println(output.toString());
	flag_hmp_tables_col_used=false;

	return f1;

	}

	public static List<List<Datum[]>> execFile(File program)throws SqlException, FileNotFoundException, ParseException
	{
		SqlParser sp = new SqlParser(new FileReader(program));
		//Object[] o = new Object[2];
		Program o = SqlParser.Program();
		List<List<Datum[]>> fin=new ArrayList<List<Datum[]>>();
		//ArrayList<PlanNode> a1=(ArrayList<PlanNode>)o[1];
		Iterator<PlanNode> it=o.queries.iterator();

		while(it.hasNext()){
			fin.add(Sql.execQuery(o.tables,it.next()));
		}
		return fin; 
	}


	public static void setTpch(){
		System.out.println("flag TPCH is set =1");
		flag_TPCH=1;
	}



	public static  void globalData(Map<String, Schema.TableFromFile> tables,PlanNode q) throws SqlException{
		Set tablesSet = tables.entrySet();
		Iterator tablesIterator = tablesSet.iterator();
		//changes for Phase3 :starts
		List ls=new ArrayList();
		ls=Utility.findCol(q);
		Iterator lsit=ls.iterator();
		String colname=null;
		String table=null;
		//String tablename1=null;
		HashMap<String,List<String>> hmp= new HashMap<String,List< String>>();
		List lscol=new ArrayList();
		while(lsit.hasNext()){
			ExprTree.VarLeaf vf=(VarLeaf) lsit.next();
			colname=vf.name.name;
			table=vf.name.rangeVariable;
			//table=tablemap.get(tablename1);
			if( table==null){
				table="nothing";
			}

			if(hmp.get(table)!=null){
				lscol=hmp.get(table);
				if(!lscol.contains(colname)){
					lscol.add(colname);
					hmp.remove(table);
					hmp.put(table, lscol);
				}
			}
			else{
				lscol= new ArrayList();
				lscol.add(colname);
				hmp.put(table, lscol);
			}

		}

		hmp_tables_col_used=hmp;
		flag_hmp_tables_col_used=true;

		//changes for Phase 3: ends
		while(tablesIterator.hasNext()){ ///loop for each table
			BufferedReader bufferedReader = null;
			String data=null;
			Datum datum = null;

			List<Datum[]> lsDatum= new ArrayList<Datum[]>();
			try{
				Map.Entry tableEntry = (Map.Entry) tablesIterator.next();
				File filename = (File)(((Schema.TableFromFile)tableEntry.getValue()).getFile());
				bufferedReader = new BufferedReader(new FileReader(filename));
				//Schema.Column column=((Schema.TableFromFile)tableEntry.getValue()).get(0); //changes for Phase 3

				String tablename=(String) tableEntry.getKey();
				//changes for Phase 3:starts
				List<Schema.Column> lscolumn= ((Schema.TableFromFile)tableEntry.getValue());
				Iterator itlscolumn=lscolumn.iterator();
				int index=-1;
				List<Integer> lsIndex= new ArrayList<Integer>();

				while(itlscolumn.hasNext()){
					index=index+1;
					Schema.Column column=(Column) itlscolumn.next();
					if(!hmp.isEmpty()&& !findColName(hmp,tablename,column.name.name) )
						continue;
					else{
						lsIndex.add(index);
					}

				}
				Iterator<Integer> ilsIndex=lsIndex.iterator();
				//changes for Phase 3:ends
				while ((data = bufferedReader.readLine()) != null){ //reading each table
					String arrtoken[];

					if(flag_TPCH==0){
						arrtoken=data.split(",");
					}
					else{
						arrtoken=data.split("\\|");
					}
					//changes for Phase 3:starts
					//Datum[] arrdatum=new Datum[arrtoken.length];
					Datum[] arrdatum=new Datum[lsIndex.size()];//changed for Phase3
					//for(int i=0;i<arrtoken.length;i++){
					int i=0;
					int j=0;
					ilsIndex=lsIndex.iterator();
					while(ilsIndex.hasNext()){ //change for Phase 3
						i=ilsIndex.next();
						//changes for Phase 3:ends
						String token=arrtoken[i];
						Schema.Column col=((Schema.TableFromFile)tableEntry.getValue()).get(i);

						if(col.type.equals(Schema.Type.INT)){
							try {
								datum= new Datum.Int(Integer.parseInt(token));
							} catch (NumberFormatException e) {
								
								//System.out.println("Not a Integer");
								if(token.contains("#")){
									System.out.println("contain #");
									token=token.substring(token.indexOf("#")+1);
									datum= new Datum.Int(Integer.parseInt(token));
								}
								else if(token.contains("-")){
									//System.out.println("can be a date");
									SimpleDateFormat df=new SimpleDateFormat("yyyy-mm-dd");
									df.setLenient(false);
									try {
										df.parse(token);
										token=token.replace("-","");
										//System.out.println("the TOKEN is :"+token);
										datum= new Datum.Int(Integer.parseInt(token));
									} catch (java.text.ParseException e1) {
										
										//e1.printStackTrace();
										System.out.println("Contains \"-\" but not a date");
									}

								}
								else
									e.printStackTrace();
							}
							Schema.Type t=datum.getType();
							//System.out.println(t);
						}
						if(col.type.equals(Schema.Type.FLOAT)){
							datum= new Flt(Float.parseFloat(token));
						}
						if(col.type.equals(Schema.Type.BOOL)){
							if(token.equals("True")){
								datum= Bool.TRUE;
							}
							else{
								datum=Bool.FALSE;
							}
						}
						if(col.type.equals(Schema.Type.STRING)){
							datum= new Str(token);
						}
						arrdatum[j]=datum;
						j=j+1;
					}//end of token array
					lsDatum.add(arrdatum);
					//lsDatum.get(0)[1].getType();

				}//end of file reading
				bufferedReader.close();
				//System.out.println(lsDatum);
				lsMapGlobalData.put(tablename, lsDatum);
				lsGlobalData.add(lsDatum);
				//Schema.Type type=lsGlobalData.get(0).get(0)[0].getType();
			}catch(FileNotFoundException e){
				e.printStackTrace();
			}catch(IOException e){
				e.printStackTrace();
			}

		}


	}

	//changes for Phase 3:starts
	public static boolean findColName(HashMap hmp,String table,String Colname){
		boolean bool=false;
		String t=tablemap.get(table);
		if(hmp.containsKey(table ) || hmp.containsKey(t)){
			List ls=(List) hmp.get(t);
			if(ls.contains(Colname)){
				bool=true;
			}	
			else{
				ls=(List) hmp.get("nothing");
				if(ls!=null && !ls.isEmpty()){
					if(ls.contains(Colname)){
						bool=true;
					}	
				}
			}
		}
		else{
			List ls=(List) hmp.get("nothing");
			if(ls!=null && !ls.isEmpty()){
				if(ls.contains(Colname)){
					bool=true;
				}
			}
		}
		return bool;
	}
	//changes for Phase 3:ends
}
