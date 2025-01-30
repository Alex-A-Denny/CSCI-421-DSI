import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                String input = scanner.nextLine();
                System.out.println("Input: " + input);
                String lower = input.toLowerCase();
                if (lower.startsWith("create")) {

                } else if (lower.startsWith("drop")) {

                } else if (lower.startsWith("alter")) {
                    
                } else if (lower.startsWith("insert into")) {
                    lower = lower.substring(11).strip();
                    System.out.println(lower);
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
                }
            }
        }
    }
}