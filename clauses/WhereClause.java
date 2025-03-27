//
// WhereClause.java
// Parses where clause, creates AST 
// and determines if a given row 
// meets where requirements
//
// Author: Alex A Denny
//
////////////////////////////////////////

package clauses;

public class WhereClause {

    static WhereTree conditionalTree;
    //make a func that takes in each data entry and the table schema,
    //read each data entry and return true or false,

    //table has all the rows
    
    public boolean passesConditional(){


        
        return true;
    }


    //make the if the where clause error if it's ambigious
    //which value applies to which tables
    //for ex if tables x(a, b, c) and y(a, f,g)
    //
    //slect a from x, y;
    //is ambigious bc we don't know where a is from
    //should be x.a or somehting
    public static void parseWhere(String userInput){
        try {
            
            String[] splitStr = userInput.split(",");
            for (String oneExpr : splitStr) {
                splitStr = oneExpr.split(" ");
                for (String singleStr : splitStr) {
                    WhereTree.buildTree(singleStr, conditionalTree);
                }
            }

            WhereTree.printTree(conditionalTree);
        } 
        catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
        
        

    }
    
}
