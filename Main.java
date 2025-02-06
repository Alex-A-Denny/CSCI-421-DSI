import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        try (Scanner scanner = new Scanner(System.in)) {

            String lower = "";//holds the input string as lowercase
                              //for the parsers

            while (scanner.hasNext()) {
                String input = scanner.next();

                if (input.toLowerCase().equals("quit")){
                    break;
                }
                System.out.println("Input: " + input);
                lower = lower.concat(input.toLowerCase() + " ");
                System.out.println("LOWER: " + lower);
                
                if(lower.strip().endsWith(";")){      //sending user
                                                             //input to parsers  
                    if(lower.startsWith("create") ||  
                        lower.startsWith("drop") || 
                        lower.startsWith("alter")){

                            //send to DDLParser
                            System.out.println("DDLParser");
                            lower = "";
                    }            
                    
                    if(lower.startsWith("insert") ||  
                        lower.startsWith("display") || 
                        lower.startsWith("select")){

                            //send to DMLParser
                            System.out.println("DMLParser");
                            lower = "";

                    }   
                }
                
                /*if (lower.startsWith("create")) {

                } else if (lower.startsWith("drop")) {

                } else if (lower.startsWith("alter")) {
                    
                } else if (lower.startsWith("insert")) {
                    
                } else if (lower.startsWith("display")) {
                    
                    String nextToken = scanner.next().toLowerCase();
                    if (nextToken.startsWith("schema")) {
                        // Display the catalog of the database
                        // TODO: Check database for catalog info
                        // Display database location, page size, buffer size, and table schema
                        System.out.println("=== DISPLAY SCHEMA ===");
                        System.out.println("DB LOCATION  : ");
                        System.out.println("PAGE SIZE    : ");
                        System.out.println("BUFFER SIZE  : ");
                        System.out.println("TABLE SCHEMA : ");

                    } else if (nextToken.startsWith("info")) {
                        // 'display info <name>'
                        String tableName = scanner.next();
                        // TODO: Check database for table info
                        // Display table name, schema, number of pages, and number of records
                        System.out.println("=== DISPLAY INFO ===");
                        System.out.println("TABLE NAME   : " + tableName);
                        System.out.println("TABLE SCHEMA : ");
                        System.out.println("# OF PAGES   : ");
                        System.out.println("# OF RECORDS : ");

                    } else {
                        System.out.println("Invalid input, Usage: 'DISPLAY SCHEMA' or 'DISPLAY INFO <name>'");
                    }

                } else if (lower.startsWith("select")) {
                    
                } else {
                    System.out.println("Invalid input");
                }*/

            }
        }
    }
}