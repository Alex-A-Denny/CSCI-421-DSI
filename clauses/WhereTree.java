//
// WhereTree.java
// AST generation for where clauses
//
// Author: Alex A Denny
//
////////////////////////////////////////

package clauses;

import java.util.ArrayList;
import java.util.List;

public class WhereTree {
    private String token;
    private String tokenType;
    private WhereTree leftChild;
    private WhereTree rightChild;
    public static WhereTree conditionalTreeRoot = null;


    public WhereTree(String tok, String type,
            WhereTree lChild, WhereTree rChild) {
        this.token = tok;
        this.tokenType = type;
        this.leftChild = lChild;
        this.rightChild = rChild;
    }

    public WhereTree(String tok, String type) {
        this.token = tok;
        this.tokenType = type;
        this.leftChild = null;
        this.rightChild = null;
    }

    public String getToken() {
        return token;
    }

    public String getTokenType() {
        return tokenType;
    }

    public WhereTree getLeftChild() {
        return leftChild;
    }

    public WhereTree getRightChild() {
        return rightChild;
    }

    public static void buildTree(Token tokenObj) {
        try {
            String tokenType = tokenObj.type;
            String tokenValue = tokenObj.value;
    
            WhereTree newNode = new WhereTree(tokenValue, tokenType);
    
            if (conditionalTreeRoot == null) {
                conditionalTreeRoot = newNode;
                return;
            }
    
            if (tokenType.equals("RelOp")) {
                if (conditionalTreeRoot.tokenType.equals("colName")) {
                    newNode.leftChild = conditionalTreeRoot;
                    conditionalTreeRoot = newNode;
                } else {
                    throw new Exception("Syntax error: RelOp must follow column name");
                }
            } else if (tokenType.equals("num") || tokenType.equals("Str") || tokenType.equals("T/F") || tokenType.equals("colName")) {
                if (conditionalTreeRoot.tokenType.equals("RelOp")) {
                    conditionalTreeRoot.rightChild = newNode;
                } else {
                    conditionalTreeRoot = newNode;
                }
            } else if (tokenType.equals("And/Or")) {
                if (tokenValue.equals("or") || (tokenValue.equals("and") && conditionalTreeRoot.tokenType.equals("or"))) {
                    newNode.leftChild = conditionalTreeRoot;
                    conditionalTreeRoot = newNode;
                } else {
                    newNode.leftChild = conditionalTreeRoot.rightChild;
                    conditionalTreeRoot.rightChild = newNode;
                }
            }
    
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static List<Token> tokenize(String input) throws Exception {
        List<Token> tokens = new ArrayList<>();
        String[] rawTokens = input.trim().split("\\s+");

        for (String token : rawTokens) {
            if (token.matches("\\d+\\.\\d+|\\d+")) {
                tokens.add(new Token("num", token));
            } else if (token.equalsIgnoreCase("true") || token.equalsIgnoreCase("false")) {
                tokens.add(new Token("T/F", token));
            } else if (token.matches("=|!=|<|<=|>|>=")) {
                tokens.add(new Token("RelOp", token));
            } else if (token.equalsIgnoreCase("and") || token.equalsIgnoreCase("or")) {
                tokens.add(new Token("And/Or", token.toLowerCase()));
            } else if (token.startsWith("\"") && token.endsWith("\"")) {
                tokens.add(new Token("Str", token));
            } else if (token.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                tokens.add(new Token("colName", token));
            } else {
                throw new Exception("Invalid token in WHERE clause: " + token);
            }
        }

        return tokens;
    }

    
    // Function to print inorder traversal
    public static void printTree(WhereTree node)
    {
        if (node == null){
            // System.out.println("null");
            return;
        }

        // First recur on left subtree

        printTree(node.leftChild);

        // Now deal with the node
        System.out.print(node.token + " ");

        // Then recur on right subtree
        printTree(node.rightChild);
    }
}
