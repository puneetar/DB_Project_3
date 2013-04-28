/**
 * A concrete implementation of a relational plan operator representing a file
 * scan operator.
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */
package edu.buffalo.cse.sql.plan;

import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;

public class IndexScanNode extends PlanNode.Leaf {
  public final String table;
  public final Schema.Table schema;
  public ExprTree condition;
  
  public ExprTree getCondition() {
	return condition;
}
public void setCondition(ExprTree condition) {
	this.condition = condition;
}
public IndexScanNode(String table, String rangeVariable, Schema.Table schema,ExprTree condition) 
  { 
    super(PlanNode.Type.INDEXSCAN); 
    this.table = table; 
    this.schema = schema.changeRangeVariable(rangeVariable);
    this.condition=condition;
  }
  public IndexScanNode(String table, Schema.Table schema,ExprTree condition) 
  {
    this(table, table, schema,condition);
  }

  public String detailString(){
    StringBuilder sb = new StringBuilder(super.detailString());
    String sep = "";
    
    sb.append(" [");
    sb.append(table);
    sb.append("(");
    for(Schema.Var v : getSchemaVars()){
      sb.append(sep);
      sb.append(v.name);
      sep = ", ";
    }
    sb.append(")]");
    
    return sb.toString();
  }

  public List<Schema.Var> getSchemaVars()
  {
    List<Schema.Var> vars = new ArrayList<Schema.Var>();
    for(Schema.Column c : schema){
      vars.add(c.name);
    }
    return vars;
  }
}
