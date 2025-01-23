import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                String input = scanner.next();
                System.out.println("Input: " + input);
                String lower = input.toLowerCase();
                if (lower.startsWith("create")) {

                } else if (lower.startsWith("drop")) {

                } else if (lower.startsWith("alter")) {
                    
                } else if (lower.startsWith("insert")) {
                    
                } else if (lower.startsWith("display")) {
                    
                } else if (lower.startsWith("select")) {
                    
                } else {
                    System.out.println("Invalid input");
                }
            }
        }
    }
}