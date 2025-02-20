//
// DMLParser.java
// Parses user commands for insert,
// display, and select
//
// Author: Alex A Denny
//
////////////////////////////////////////



public class DMLParser {


    class Attribute{

    }

    static class CreateTable{
        int id;
        String tableName;

    }


    public static void parseCreateTable(String input){

        System.out.println(input);
        CreateTable ct = new CreateTable();
        ct.tableName = input;
    }
}
