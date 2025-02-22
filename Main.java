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
import catalog.Catalog;
import page.RecordCodec;
import page.RecordEntry;
import page.RecordEntryType;
import storage.PageBuffer;
import storage.StorageManager;
import table.TableSchema;

public class Main {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java Main <db loc> <page size> <buffer size>");
            return;
        }

        String dbPathRaw = args[0];
        int pageSize;
        try {
            pageSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Unable to parse page size");
            System.exit(1);
            return;
        }
        int pageBufferSize;
        try {
            pageBufferSize = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Unable to parse page buffer size");
            System.exit(1);
            return;
        }

        Path dbPath = Paths.get(dbPathRaw);
        if (!Files.exists(dbPath)) {
            try {
                Files.createDirectories(dbPath);
            } catch (IOException e) {
                System.err.println("Unable to create path to database");
                e.printStackTrace();
                System.exit(1);
                return;
            }
        }

        Path catalogPath = dbPath.resolve("catalog");
        Catalog catalog;
        if (Files.exists(catalogPath)) {
            byte[] raw;
            try {
                raw = Files.readAllBytes(catalogPath);
            } catch (IOException e) {
                System.err.println("Error reading catalog");
                e.printStackTrace();
                System.exit(1);
                return;
            }
            ByteBuffer buf = ByteBuffer.wrap(raw);
            buf.rewind();
            catalog = Catalog.decode(buf);
        } else {
            catalog = new Catalog(pageSize);
        }

        PageBuffer pageBuffer = new PageBuffer(dbPath, pageSize, pageBufferSize);
        StorageManager storageManager = new StorageManager(catalog, pageBuffer);

        DDLParser ddl = new DDLParser(catalog, storageManager);
        DMLParser dml = new DMLParser(storageManager);
    
        try (Scanner scanner = new Scanner(System.in)) {
            //DDLParser DDL = new DDLParser();

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
                            // ddl.parseCreateTable(lower);
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