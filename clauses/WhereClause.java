package clauses;

public class WhereClause {

    String conditional;


    //make a func that takes in each data entry and the table schema,
    //read each data entry and return true or false,

    
    public boolean passesConditional(){


        conditional = "";
        return true;
    }


    //make the if the where clause error if it's ambigious
    //which value applies to which tables
    //for ex if tables x(a, b, c) and y(a, f,g)
    //
    //slect a from x, y;
    //is ambigious bc we don't know where a is from
    //should be x.a or somehting
    public void parseWhere(String userInput){
        conditional = "";
        conditional = userInput;
        System.out.println(conditional);

    }
    
}
