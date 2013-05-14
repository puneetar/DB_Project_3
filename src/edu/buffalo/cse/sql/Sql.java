/* Generated By:JavaCC: Do not edit this line. Sql.java */
package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.*;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Index.IndexType;
import edu.buffalo.cse.sql.Schema.Column;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.Bool;
import edu.buffalo.cse.sql.data.Datum.Flt;
import edu.buffalo.cse.sql.data.Datum.Str;

import edu.buffalo.cse.sql.optimizer.PlanRewrite;
import edu.buffalo.cse.sql.optimizer.PushDownSelects;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;
import edu.buffalo.cse.sql.plan.Expression;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.util.*;
public class Sql {
	public static List<List<Datum[]>> lsGlobalData = new ArrayList<List<Datum[]>>();
	public static HashMap<String, List<Datum[]>> lsMapGlobalData= new HashMap<String, List<Datum[]>>();
	public static HashMap<String,List<String>> hmp_tables_col_used= null;
	public static boolean flag_hmp_tables_col_used=false;


	private static int flag_limit = 0;
	private static Map<Datum,ArrayList<Datum[]>> globalMap = new TreeMap<Datum,ArrayList<Datum[]>>();
	private static HashMap<String,Integer> orderByMap = new HashMap<String,Integer>();
	private static List<String> orderByList = new ArrayList<String>();
	public static int flag_TPCH=0;
	public static HashMap<String,String> tablemap= new HashMap<String,String>();

	public static int flag_index=0;

	public static void main( String[] args )
	{

		for(String arg: args){
			if(arg.equalsIgnoreCase("-index")){
				flag_index=1;
				break;
			}
		}

		try {
			List<List<Datum[]>> result=Sql.execFile(new File(flag_index==1?args[1]:args[0]));

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
	System.out.println(q);
	PlanNode newq=objPush.rewrite(q);
	System.out.println(newq);
	q=newq;
	globalData(tables,q);

	List<Datum[]> f1 = new ArrayList<Datum[]>();
	List<Datum[]> f = new ArrayList<Datum[]>();

	System.out.println("GLOBAL data made");

	///----------------------Utkarsh code : Order By
	List<Datum[]> temp=Utility.switchNodes(q);
	if(orderByMap.isEmpty()){
		f = temp;
	}
	else{
		String col1 = orderByList.get(0);
		String table = null;
		String column = null;
		if(col1.contains(".")){
			String[] schema = col1.split("\\.");
			table = schema[0]; column = schema[1];
		}
		else{
			column = col1;
		}
		List<Schema.Var> list=q.getSchemaVars();
		Iterator<Schema.Var> it=list.iterator();
		int i = 0;
		while(it.hasNext()){
			Schema.Var sc = it.next();
			if(table!=null){
				if(sc.name.equals(column) && sc.rangeVariable.equals(table)) //check
					break;
			}
			else{
				if(sc.name.equals(column))
					break;
			}
			i++;
		}
		if(orderByMap.get(col1)==1){
			Map<Datum,ArrayList<Datum[]>> tm = new TreeMap<Datum,ArrayList<Datum[]>>();
			tm = sortDecreasing(temp,i);
			f = prepare(tm);
			globalMap = tm;
		}
		else{
			Map<Datum,ArrayList<Datum[]>> tm = new TreeMap<Datum,ArrayList<Datum[]>>();
			tm = sortIncreasing(temp,i);
			f = prepare(tm);
			globalMap = tm;
		}
		if(orderByList.size()==2){
			String col2 = orderByList.get(1);
			String table1 = null;
			String column1 = null;
			if(col2.contains(".")){
				String[] schema = col2.split(".");
				table1 = schema[0]; column1 = schema[1];
			}
			else{
				column1 = col2;
			}
			Iterator<Schema.Var> iterator = list.iterator();
			int j = 0;
			while(iterator.hasNext()){
				Schema.Var sc = iterator.next();
				if(table1!=null){
					if(sc.name.equals(column1) && sc.rangeVariable.equals(table1)) //check
						break;
				}
				else{
					if(sc.name.equals(column1))
						break;
				}
				j++;
			}
			Map<Datum,ArrayList<Datum[]>> tm1 = new TreeMap<Datum,ArrayList<Datum[]>>();
			Map<Datum,ArrayList<Datum[]>> finalMapinc = new TreeMap<Datum,ArrayList<Datum[]>>();
			Map<Datum,ArrayList<Datum[]>> finalMapdec = new TreeMap<Datum,ArrayList<Datum[]>>(Collections.reverseOrder());
			for(Map.Entry<Datum,ArrayList<Datum[]>> entry : globalMap.entrySet()){
				ArrayList<Datum[]> al = entry.getValue();
				ArrayList<Datum[]> newDatumList = new ArrayList<Datum[]>();
				if(al.size()>1){
					tm1 = sortIncreasing(al,j);
					newDatumList = prepare(tm1);
					if(orderByMap.get(col1)==1)
						finalMapdec.put(entry.getKey(), newDatumList);
					else
						finalMapinc.put(entry.getKey(), newDatumList);
				}
				else{
					if(orderByMap.get(col1)==1)
						finalMapdec.put(entry.getKey(), newDatumList);
					else
						finalMapinc.put(entry.getKey(), newDatumList);
				}
			}
			if(orderByMap.get(col1)==1)
				f = prepare(finalMapdec);
			else
				f = prepare(finalMapinc);
		}
	}
	if(flag_limit==0)
		f1 = f;
	else
		f1 = f.subList(0, flag_limit);
	//----------------UTKARSH code : end Order By && LIMIT

	//	TableBuilder output = new TableBuilder();
	//	List<Schema.Var> list=q.getSchemaVars();
	//	Iterator<Schema.Var> it=list.iterator();
	//	while(it.hasNext()){
	//		Schema.Var sc=it.next();
	//		// output.newCell(sc.name);
	//		System.out.println(sc.name+" : "+sc.rangeVariable);
	//	}
	//	Iterator<Datum[]> resultIterator=f1.iterator();
	//
	//	output.addDividerLine();
	//	while(resultIterator.hasNext()){
	//		Datum[] row = resultIterator.next();
	//		output.newRow();
	//		for(Datum d : row){
	//			output.newCell(d.toString());
	//		}
	//	}
	//
	//	System.out.println(output.toString());
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

		if(flag_index==1){
			List<ExprTree> lsnodes=Utility.findIndexNodes(q);
			//changes for Phase 3: ends
			HashMap<Schema.TableFromFile,int[]> hmpindex= new HashMap<Schema.TableFromFile,int[]>();
			while(tablesIterator.hasNext()){
				BufferedReader bufferedReader = null;
				String data=null;
				Datum datum = null;
				List<Datum[]> lsDatum= new ArrayList<Datum[]>();
				Map.Entry tableEntry1 = (Map.Entry) tablesIterator.next();
				String tablename=(String) tableEntry1.getKey();
				Schema.TableFromFile tf=(Schema.TableFromFile)tableEntry1.getValue();
				List<Schema.Column> lscolumn= ((Schema.TableFromFile)tableEntry1.getValue());
				String t=Sql.tablemap.get(tablename);
				Iterator itrnodes=lsnodes.iterator();
				//List<Integer> lsIndex= new ArrayList<Integer>();
				List<IndexCondition> lsic= new ArrayList<IndexCondition>();
				ExprTree.OpCode op=null;
				while(itrnodes.hasNext()){
					ExprTree exp=(ExprTree) itrnodes.next();
					op=exp.op;
					List ls1=new Expression(exp).findColumns();

					Datum[] d=new Datum[1];
					ExprTree.ConstLeaf cf=(ExprTree.ConstLeaf)exp.get(1);
					d[0]=cf.v;

					ExprTree.VarLeaf vf= (ExprTree.VarLeaf)ls1.get(0);
					if(t!=null){
						if(t.equals(vf.name.rangeVariable)){
							int index1=-1;
							Iterator<Column> it=lscolumn.iterator();

							while(it.hasNext()){
								index1=index1+1;
								Schema.Column column=(Column) it.next();
								if(column.name.equals(vf.name)){
									IndexCondition ic= new  IndexCondition(index1,op ,d[0]);
									lsic.add(ic);
									//									if(!lsIndex.contains(index1))
									//										lsIndex.add(index1);
								}
							}

						}
					}

				}
				
					try {
						if(lsic.size()>0){
							Iterator<IndexCondition> it_lsic=lsic.iterator();
							int arr_key[]=new int[lsic.size()];
							Datum arr_value[]=new Datum[lsic.size()];
							ExprTree.OpCode arr_opCode[]=new ExprTree.OpCode[lsic.size()]; 
							int i=0;
							while(it_lsic.hasNext()){
								arr_key[i++]=it_lsic.next().getIndex();
								arr_value[i++]=it_lsic.next().getValue();
								arr_opCode[i++]=it_lsic.next().getOpCode();
							}
							Index.createIndex(tf, findType(lsic.get(0).getOpCode()), arr_key);
							
							switch(findType(lsic.get(0).getOpCode())){
							case HASH:
								List<Datum[]> lsIndexDatum=Index.getFromIndex(tf, IndexType.HASH, arr_key, arr_value);
								lsMapGlobalData.put(tablename, lsIndexDatum);
								lsGlobalData.add(lsIndexDatum);
								break;
							case ISAM:
//								if(lsic.size()==2){
//									
//								}
//								else if(lsic.size()==1){
//									
//								}
								
								
								break;
							default:
								break;
							}
							
						}
						else{
							File filename = (File)tf.getFile();
							bufferedReader = new BufferedReader(new FileReader(filename));
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
									Schema.Column col=tf.get(i);

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
										//Schema.Type t=datum.getType();
										//System.out.println(t);
									}
									else if(col.type.equals(Schema.Type.FLOAT)){
										datum= new Flt(Float.parseFloat(token));
									}
									else if(col.type.equals(Schema.Type.BOOL)){
										if(token.equals("True")){
											datum= Bool.TRUE;
										}
										else{
											datum=Bool.FALSE;
										}
									}
									else if(col.type.equals(Schema.Type.STRING)){
										datum= new Str(token);
									}
									arrdatum[j]=datum;
									j=j+1;
								}//end of token array
								lsDatum.add(arrdatum);
								//lsDatum.get(0)[1].getType();

							}
							bufferedReader.close();
							lsMapGlobalData.put(tablename, lsDatum);
							lsGlobalData.add(lsDatum);
						}


					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//hmpindex.put(tf, arr);
				}

			

		}
		else{

		//tablesIterator = tablesSet.iterator();

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
							//Schema.Type t=datum.getType();
							//System.out.println(t);
						}
						else if(col.type.equals(Schema.Type.FLOAT)){
							datum= new Flt(Float.parseFloat(token));
						}
						else if(col.type.equals(Schema.Type.BOOL)){
							if(token.equals("True")){
								datum= Bool.TRUE;
							}
							else{
								datum=Bool.FALSE;
							}
						}
						else if(col.type.equals(Schema.Type.STRING)){
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

	public static IndexType findType(ExprTree.OpCode op){
		if( op.equals(OpCode.EQ)){
			return IndexType.HASH;
		}
		else
			return IndexType.ISAM;
	}		



	//UTKARSK methods : for Order BY && LIMIT
	public static void setLimit(int lim){
		flag_limit = lim;
	}

	public static void setOrderMap(HashMap<String,Integer> map){
		orderByMap = map;
	}

	public static void setOrderList(List<String> list){
		orderByList = list;
	}

	public static Map<Datum, ArrayList<Datum[]>> sortIncreasing(List<Datum[]> temp,int i){
		Map<Datum,ArrayList<Datum[]>> tm = new TreeMap<Datum,ArrayList<Datum[]>>();
		Iterator<Datum[]> it1 = temp.iterator();
		while(it1.hasNext()){
			Datum[] d = it1.next();
			if(tm.containsKey(d[i])){
				ArrayList<Datum[]> l = tm.get(d[i]);
				l.add(d);
			}
			else{
				ArrayList<Datum[]> l = new ArrayList<Datum[]>();
				l.add(d);
				tm.put(d[i], l);
			}
		}
		return tm;
	}

	public static Map<Datum, ArrayList<Datum[]>> sortDecreasing(List<Datum[]> temp,int i){
		Map<Datum,ArrayList<Datum[]>> tm = new TreeMap<Datum,ArrayList<Datum[]>>(Collections.reverseOrder());
		Iterator<Datum[]> it1 = temp.iterator();
		while(it1.hasNext()){
			Datum[] d = it1.next();
			if(tm.containsKey(d[i])){
				ArrayList<Datum[]> l = tm.get(d[i]);
				l.add(d);
			}
			else{
				ArrayList<Datum[]> l = new ArrayList<Datum[]>();
				l.add(d);
				tm.put(d[i], l);
			}
		}
		return tm;
	}

	public static ArrayList<Datum[]> prepare(Map<Datum,ArrayList<Datum[]>> tm){
		ArrayList<Datum[]> f = new ArrayList<Datum[]>();
		for(Map.Entry<Datum,ArrayList<Datum[]>> entry : tm.entrySet()){
			ArrayList<Datum[]> l = entry.getValue();
			Iterator<Datum[]> it2 = l.iterator();
			while(it2.hasNext()){
				Datum[] lDatum = it2.next();
				f.add(lDatum);
			}
		}
		return f;

	}
}
