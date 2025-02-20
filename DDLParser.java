public class DDLParser {
 //takes in a string     
    public static void main (String[] args){
        System.out.println("DDLParser called");
        if (args[0] == "drop"){
            dropTable(args);
        }
        else if (args[0] == "create"){
            // do something
        }
        else if (args[0] == "alter"){
            // do something
        }
        else{
            System.out.println("DDL command not recognized");
        }
    }

    public static void dropTable(String[] args){
        System.out.println("Now droping table" + args[2]);
        String drop_statement = "drop table" + args[2];
        System.out.println("Executing: "+ drop_statement);
    }
}
