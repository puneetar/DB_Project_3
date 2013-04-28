package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Type;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.util.ManageList;
import edu.buffalo.cse.sql.util.Utility;

public class Expression extends ExprTree {

	List<Datum[]> data;
	List<Schema.Var> schemaOfData;
	ExprTree expr;

	public Expression(ExprTree expr){
		super(expr.op);
		this.expr=expr;
	}

	public Expression(ExprTree expr,List<Datum[]> data,List<Schema.Var> schema){
		super(expr.op);
		this.data=data;
		this.schemaOfData=schema;
		this.expr=expr;
	}

	public Expression(OpCode op,List<Datum[]> data,List<Schema.Var> schema,ExprTree expr) {
		super(op);
		this.data=data;
		this.schemaOfData=schema;
		this.expr=expr;
		// TODO Auto-generated constructor stub
	}

	public Expression(OpCode op, ExprTree child,List<Datum[]> data,List<Schema.Var> schema,ExprTree expr) {
		super(op, child);
		this.data=data;
		this.schemaOfData=schema;
		this.expr=expr;
		// TODO Auto-generated constructor stub
	}

	public Expression(OpCode op, ExprTree lhs, ExprTree rhs,List<Datum[]> data,List<Schema.Var> schema,ExprTree expr) {
		super(op, lhs, rhs);
		this.data=data;
		this.schemaOfData=schema;
		this.expr=expr;
		// TODO Auto-generated constructor stub
	}


	public List<Datum[]> doExpr() throws CastError{

		ArrayList<Datum[]> ret=new ArrayList<Datum[]>();
		int y=0,z=0;
		switch (this.op) {
		case ADD:{

			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array

			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Int(arrLHS[y].toInt()+arrRHS[z].toInt());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case FLOAT:
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Flt(arrLHS[y].toFloat()+arrRHS[z].toFloat());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				System.out.println("cannot call ADD operation on string ");
				break;
			case BOOL: 
				System.out.println("cannot call ADD operation on boolean ");
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//	System.out.println((ret!=null)?ret.toString():"Error : ret is NULL");

			return new ManageList().toListOfDatumArray(result);

		}

		case MULT:{
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Int(arrLHS[y].toInt()*arrRHS[z].toInt());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case FLOAT:
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Flt(arrLHS[y].toFloat()*arrRHS[z].toFloat());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				System.out.println("cannot call MULTIPLY operation on string ");
				break;
			case BOOL: 
				System.out.println("cannot call MULTIPLY operation on boolean ");
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			return new ManageList().toListOfDatumArray(result);

		}

		case SUB:{
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Int(arrLHS[y].toInt()-arrRHS[z].toInt());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case FLOAT:
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Flt(arrLHS[y].toFloat()-arrRHS[z].toFloat());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				System.out.println("cannot call SUBTRACT operation on string ");
				break;
			case BOOL: 
				System.out.println("cannot call SUBTRACT operation on boolean ");
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			return new ManageList().toListOfDatumArray(result);

		}
		case DIV: {
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Int(arrLHS[y].toInt()/arrRHS[z].toInt());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case FLOAT:
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Flt(arrLHS[y].toFloat()/arrRHS[z].toFloat());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				System.out.println("cannot call DIVIDE operation on string ");
				break;
			case BOOL: 
				System.out.println("cannot call DIVIDE operation on boolean ");
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			return new ManageList().toListOfDatumArray(result);

		}
		case AND: {
			if(!(expr.size()>=2))
				break;

			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];




			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
			case FLOAT:
			case STRING:  
				for(int i=0;i<lhs.size();i++){
					for(int j=0;j<rhs.size();j++){
						if(compare(lhs.get(i), rhs.get(j))){
							ret.add(lhs.get(i));
							break;
						}
					}
				}

				return ret;

			case BOOL:
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Bool(arrLHS[y].toBool()&&arrRHS[z].toBool());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				return new ManageList().toListOfDatumArray(result);
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//			

		}
		case OR:  {
			if(!(expr.size()>=2))
				break;
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();




			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
			case FLOAT:
			case STRING:  
				for(int i=0;i<lhs.size();i++){
					for(int j=0;j<rhs.size();j++){
						if(compare(lhs.get(i), rhs.get(j))){
							lhs.remove(i);
							break;
						}
					}
				}

				for(Datum[] d:lhs){
					ret.add(d);
				}
				for(Datum[] d:rhs){
					ret.add(d);
				}
				return ret;

			case BOOL:
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Bool(arrLHS[y].toBool()||arrRHS[z].toBool());
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				return new ManageList().toListOfDatumArray(result);
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}



		}
		case NOT: 
		{
			List<Datum[]> child=(new Expression(expr.get(0),data,schemaOfData)).doExpr();

			Datum[] arrchild=new ManageList(child).getColumn(0);

			Datum[] result=new Datum[arrchild.length];


			switch(arrchild[0].getType()){
			case INT: 
				System.out.println("cannot call NOT operation on int ");
				break;
			case FLOAT:
				System.out.println("cannot call NOT operation on float ");
				break;
			case STRING:  
				System.out.println("cannot call NOT operation on string ");
				break;
			case BOOL:
				for(int i=0;i<result.length;i++){
					result[i]=new Datum.Bool(!arrchild[i].toBool());
				}
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			return new ManageList().toListOfDatumArray(result);

		}

		case EQ:  {
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			int countResult=0;
			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toInt()==arrRHS[z].toInt()){
						result[countResult]=new Datum.Int(arrLHS[y].toInt());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;

				}
				break;

			case FLOAT:
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toFloat()==arrRHS[z].toFloat()){
						result[countResult]=new Datum.Flt(arrLHS[y].toFloat());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))==0){
						result[countResult]=new Datum.Str(arrLHS[y].toOnlyString());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case BOOL:
				for(int i=0;i<result.length;i++){
					if(new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))==0){
						result[countResult]=new Datum.Bool(arrLHS[y].toBool());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//return new ManageList().toListOfDatumArray(Arrays.copyOfRange(result, 0, countResult));
			return ret;
		}
		case NEQ:  {
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			int countResult=0;
			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toInt()!=arrRHS[z].toInt()){
						result[countResult]=new Datum.Int(arrLHS[y].toInt());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;

				}
				break;

			case FLOAT:
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toFloat()!=arrRHS[z].toFloat()){
						result[countResult]=new Datum.Flt(arrLHS[y].toFloat());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))!=0){
						result[countResult]=new Datum.Str(arrLHS[y].toOnlyString());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case BOOL:
				for(int i=0;i<result.length;i++){
					if(new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))!=0){
						result[countResult]=new Datum.Bool(arrLHS[y].toBool());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//return new ManageList().toListOfDatumArray(Arrays.copyOfRange(result, 0, countResult));
			return ret;
		}
		case LT:   {
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			int countResult=0;
			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toInt()<arrRHS[z].toInt()){
						result[countResult]=new Datum.Int(arrLHS[y].toInt());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;

				}
				break;

			case FLOAT:
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toFloat()<arrRHS[z].toFloat()){
						result[countResult]=new Datum.Flt(arrLHS[y].toFloat());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))==-1){
						result[countResult]=new Datum.Str(arrLHS[y].toOnlyString());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case BOOL:
				for(int i=0;i<result.length;i++){
					if(new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))==-1){
						result[countResult]=new Datum.Bool(arrLHS[y].toBool());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//return new ManageList().toListOfDatumArray(Arrays.copyOfRange(result, 0, countResult));
			return ret;
		}

		case GT:   {
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			int countResult=0;
			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toInt()>arrRHS[z].toInt()){
						result[countResult]=new Datum.Int(arrLHS[y].toInt());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;

				}
				break;

			case FLOAT:
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toFloat()>arrRHS[z].toFloat()){
						result[countResult]=new Datum.Flt(arrLHS[z].toFloat());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))==1){
						result[countResult]=new Datum.Str(arrLHS[y].toOnlyString());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case BOOL:
				for(int i=0;i<result.length;i++){
					if(new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))==1){
						result[countResult]=new Datum.Bool(arrLHS[y].toBool());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//return new ManageList().toListOfDatumArray(Arrays.copyOfRange(result, 0, countResult));
			return ret;
		}
		case LTE:   {
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			int countResult=0;
			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toInt()<=arrRHS[z].toInt()){
						result[countResult]=new Datum.Int(arrLHS[y].toInt());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;

				}
				break;

			case FLOAT:
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toFloat()<=arrRHS[z].toFloat()){
						result[countResult]=new Datum.Flt(arrLHS[y].toFloat());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))==-1||arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))==0){
						result[countResult]=new Datum.Str(arrLHS[y].toOnlyString());
						countResult++;
						ret.add(data.get(i));
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case BOOL:
				for(int i=0;i<result.length;i++){
					if(new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))==-1||new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))==0){
						result[countResult]=new Datum.Bool(arrLHS[y].toBool());
						countResult++;
						ret.add(data.get(i));
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//	return new ManageList().toListOfDatumArray(Arrays.copyOfRange(result, 0, countResult));
			return ret;
		}
		case GTE:   {
			List<Datum[]> lhs=(new Expression(expr.get(0),data,schemaOfData)).doExpr();
			List<Datum[]> rhs=(new Expression(expr.get(1),data,schemaOfData)).doExpr();

			Datum[] arrLHS=new ManageList(lhs).getColumn(0);
			Datum[] arrRHS=new ManageList(rhs).getColumn(0);

			//assuming that the size of LHS and RHS array
			Datum[] result=new Datum[arrLHS.length>arrRHS.length?arrLHS.length:arrRHS.length];

			int countResult=0;
			Schema.Type t;

			if(arrLHS[0].getType()==Schema.Type.FLOAT ||arrRHS[0].getType()==Schema.Type.FLOAT)
				t=Schema.Type.FLOAT;
			else
				t=arrLHS[0].getType();

			switch(t){
			case INT: 
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toInt()>=arrRHS[z].toInt()){
						result[countResult]=new Datum.Int(arrLHS[y].toInt());
						ret.add(data.get(i));
						countResult++;
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;

				}
				break;

			case FLOAT:
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toFloat()>=arrRHS[z].toFloat()){
						result[countResult]=new Datum.Flt(arrLHS[y].toFloat());
						countResult++;
						ret.add(data.get(i));
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case STRING:  
				for(int i=0;i<result.length;i++){
					if(arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))==1||arrLHS[y].toOnlyString().compareTo((arrRHS[z].toOnlyString()))==0){
						result[countResult]=new Datum.Str(arrLHS[y].toOnlyString());
						countResult++;
						ret.add(data.get(i));
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			case BOOL:
				for(int i=0;i<result.length;i++){
					if(new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))==1||new Boolean(arrLHS[y].toBool()).compareTo(new Boolean(arrRHS[z].toBool()))==0){
						result[countResult]=new Datum.Bool(arrLHS[y].toBool());
						countResult++;
						ret.add(data.get(i));
					}
					if(y<arrLHS.length-1)
						y++;
					if(z<arrRHS.length-1)
						z++;
				}
				break;
			default:
				System.out.println("\nCould not fine Type of Datum");
				break;
			}

			//return new ManageList().toListOfDatumArray(Arrays.copyOfRange(result, 0, countResult));
			return ret;
		}
		case CONST: 
			Datum[] d=new Datum[1];
			ExprTree.ConstLeaf cf=(ExprTree.ConstLeaf)this.expr;

			for(StackTraceElement e:Thread.currentThread().getStackTrace())
			{
				if(e.getClassName().equals("edu.buffalo.cse.sql.plan.Project"))
				{
					d[0]=cf.v;
					return new ManageList().toListOfDatumArray(d);
				}
				else if(e.getClassName().equals("edu.buffalo.cse.sql.plan.Aggregate"))
				{
					return new ManageList().toListOfDatumArray(new ManageList(data).getColumn(cf.v.toInt()));
				}
			}
			//			if(schemaOfData.isEmpty()){
			//				d[0]=cf.v;
			//				return new ManageList().toListOfDatumArray(d);	
			//			}
			//			else{
			//				return new ManageList().toListOfDatumArray(new ManageList(data).getColumn(cf.v.toInt()));
			//			}

		case VAR: {
			ExprTree.VarLeaf vf= (ExprTree.VarLeaf)this.expr;
			Schema.Var schema=vf.name;
			int index=schemaOfData.indexOf(vf);

			Iterator<Var> it=schemaOfData.iterator();
			int i=0;
			while(it.hasNext()){
				if(it.next().equals(vf.name))
					index=i;
				i++;
			}

			// I can use  .indexOf(vf) 
			//		OR
			// I can iterate over the schemaOfData and compare it with vf with method of Schema.Var equlas(). 

			//	index=0;
			//	System.out.println("found the Schema : "+schemaOfData.indexOf(vf));
			//System.out.println(index);
			String rangeVariable = vf.name.rangeVariable;
			String name= vf.name.name;
			//System.out.println("hell");
			//System.out.println(data);
			return new ManageList().toListOfDatumArray(new ManageList(data).getColumn(index));

		}
		//	break;
		default: System.out.println("Error in Expression class");
		break;
		}
		return ret;
	}

	private boolean compare(Datum[] lhs,Datum[] rhs){

		//System.out.println((lhs.length==rhs.length)?"\n*********\n":"\n*****they are not equal**** ");

		try {
			for(int i=0;i<lhs.length;i++){
				if(lhs[i].equalsImpl(rhs[i])){
					//continue;
				}
				else{
					return false;
				}

			}
		} catch (CastError e) {
			// TODO Auto-generated catch block
			//System.out.println("***************Cast Error in Expression.compare() ");
			e.printStackTrace();
		}

		return true;




	}

	public List findColumns(){
		List ls=new ArrayList();
		List ls1=new ArrayList();
		List ls2=new ArrayList();
		Iterator it1=null;
		Iterator it2=null;
		switch(this.expr.op){
		case ADD:
		case MULT:
		case DIV:
		case SUB:
		case EQ:
		case NEQ:
		case GTE:
		case LTE:
		case GT:
		case LT:
		case AND:
		case OR:{
			if(!(expr.size()>=2)){
				ls1=new Expression(this.expr.get(0)).findColumns();
				it1=ls1.iterator();
				while(it1.hasNext()){
					ls.add(it1.next());
				}
				break;
			}
			ls1=new Expression(this.expr.get(0)).findColumns();
			ls2=new Expression(this.expr.get(1)).findColumns();
			
			
			it1=ls1.iterator();
			while(it1.hasNext()){
				ls.add(it1.next());
			}
			it2=ls2.iterator();
			while(it2.hasNext()){
				ls.add(it2.next());
			}
			break;
		}
		case NOT:{
			ls1=new Expression(this.expr.get(0)).findColumns();
			it1=ls1.iterator();
			while(it1.hasNext()){
				ls.add(ls1.get(0));
			}
			break;
		}
		case CONST:{

			break;
		}
		case VAR:{
			ExprTree.VarLeaf vf= (ExprTree.VarLeaf)this.expr;
			ls.add(vf);
			break;
		}
		default:{
			System.out.println(" Cannot find Columns in EXPRESSIOn");
		}

		}
		return ls;
	}


}