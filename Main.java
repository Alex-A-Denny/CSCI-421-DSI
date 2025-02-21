//
// Main.java
// Initial parsing, sends user input
// to DDL and DML parsers
//
// Author: Alex A Denny
//
////////////////////////////////////////



import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
//import DMLParser;

import catalog.Catalog;
import page.RecordCodec;
import page.RecordEntry;
import page.RecordEntryType;
import storage.PageBuffer;
import storage.StorageManager;
import table.TableSchema;

public class Main {

        public static void main(String[] args) {
            try (Scanner scanner = new Scanner(System.in)) {
    
                String lower = "";//holds the input string as lowercase
                                  //for the parsers
    
                while (scanner.hasNext()) {
                    String input = scanner.next();
    
                    if (input.toLowerCase().equals("quit") && lower.isEmpty()){
                        break;
                    }
                    //System.out.println("Input: " + input);
                    lower = lower.concat(input.toLowerCase() + " ");
                    //System.out.println("LOWER: " + lower);
                    
                    if(lower.strip().endsWith(";")){      //sending user
                                                                 //input to parsers  
                        if(lower.startsWith("create") ||  
                            lower.startsWith("drop") || 
                            lower.startsWith("alter")){
                                //send to DDLParser
                                DMLParser.parseCreateTable(lower);
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