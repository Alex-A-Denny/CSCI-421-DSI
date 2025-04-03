//
// Main.java
// Initial parsing, sends user input
// to DDL and DML parsers
//
// Author: Alex A Denny
//
////////////////////////////////////////



import catalog.Catalog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import storage.PageBuffer;
import storage.StorageManager;

public class Main {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: java Main <db loc> <page size> <buffer size> <indexing>");
            return;
        }

        String dbPathRaw = args[0];
        int pageSize;
        try {
            pageSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Error: Unable to parse page size");
            System.exit(1);
            return;
        }
        int pageBufferSize;
        try {
            pageBufferSize = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Error: Unable to parse page buffer size");
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
            pageSize = catalog.getPageSize();
        } else {
            catalog = new Catalog(pageSize);
        }

        //Indexing
        if(args[3].toLowerCase().equals("true")){
            //we are using indexing
        }
        
        PageBuffer pageBuffer = new PageBuffer(dbPath, pageSize, pageBufferSize);
        StorageManager storageManager = new StorageManager(catalog, pageBuffer);

        DDLParser ddl = new DDLParser(catalog, storageManager);
        DMLParser dml = new DMLParser(storageManager);
    
        try (Scanner scanner = new Scanner(System.in)) {
            

            String query = "";//holds the total userinput string 
                              //for the parsers

            while (scanner.hasNext()) {
                String input = scanner.next();

                if (input.toLowerCase().equals("quit") && query.isEmpty()){
                    break;
                }
                //System.out.println("Input: " + input);
                query = query.concat(input + " ");
                //System.out.println("QUERY: " + query);
                
                if(query.strip().endsWith(";")){
                    //Input gathering done, sending user input to parsers  
                    query = query.substring(0, query.lastIndexOf(";")).trim();

                    //send to DDL Parser
                    if(query.toLowerCase().startsWith("create")){
                            
                        ddl.parseCreateTable(query);
                        query = "";
                    } else if(query.toLowerCase().startsWith("drop")){
                        ddl.parseDropTable(query);
                        query = "";
                    } else if(query.toLowerCase().startsWith("alter")){
                        ddl.parseAlterTable(query.toLowerCase());
                        query = "";
                    }
                    //send to DMLParser
                    else if(query.toLowerCase().startsWith("insert")){

                        dml.parseInsert(query);
                        query = "";

                    } else if(query.toLowerCase().startsWith("display") ){
                        dml.parseDisplay(query);
                        query = "";
                    } else if(query.toLowerCase().startsWith("select")){
                        dml.parseSelect(query);
                        query = "";
                    } else if (query.toLowerCase().startsWith("delete")) {
                        dml.parseDelete(query);
                        query = "";
                    }
                    else if (query.toLowerCase().startsWith("update")){
                        dml.parseUpdate(query);
                        query = "";
                    }else {
                        query = "";
                        System.out.println("Unknown command. Query must start with (CREATE, DROP, ALTER, INSERT, DISPLAY, or SELECT)");
                    }
                }
            }
        }

        // write everything to the disk before exit
        ByteBuffer encodedCatalog = catalog.encode();
        try {
            Files.write(catalogPath, encodedCatalog.array());
        } catch (IOException e) {
            System.err.println("Error: Unable to write catalog");
            e.printStackTrace();
            System.exit(1);
            return;
        }
        try {
            pageBuffer.purge();
        } catch (IOException e) {
            System.err.println("Error: Unable to purge page buffer");
            e.printStackTrace();
            System.exit(1);
            return;
        }
    }
}