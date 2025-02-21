public class DDLParser {
 //takes in a string//
// DDLParser.java
// Parses user commands for insert,
// display, and select
//
// Author: Alex A Denny
//
////////////////////////////////////////



public class DDLParser {


    class Attribute{

    }

    static class CreateTable{
        int id;
        String tableName;

    }


   
    public static void parseCreateTable(String input) {
        System.out.println(input);
            CreateTable ct = new CreateTable();
            ct.tableName = input;
    }
    
}

   
}
