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

            }
        }
    }
}