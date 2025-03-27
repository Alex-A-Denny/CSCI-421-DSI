//
// WhereTree.java
// AST generation for where clauses
//
// Author: Alex A Denny
//
////////////////////////////////////////

package clauses;

public class WhereTree {
    private String token;
    private String tokenType;
    private WhereTree leftChild;
    private WhereTree rightChild;

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

    public static void buildTree(String singleStr, WhereTree tree) {
        try {

            //determining type of token
            String tokenType = "";
            if (singleStr.contains("\"")) {
                tokenType = "Str";
            } 
            else if (singleStr.equals("=") || singleStr.equals(">") || singleStr.equals("<") ||
                    singleStr.equals(">=") || singleStr.equals("<=") || singleStr.equals("!=")) {
                tokenType = "RelOp";
            } 
            else if (singleStr.equals("true") || singleStr.equals("false")) {
                tokenType = "T/F";
            } 
            else if (singleStr.equals("and") || singleStr.equals("or")) {
                tokenType = "And/Or";
            } 
            else if (singleStr.matches("^[a-zA-Z]+$")) {
                tokenType = "colName";
            }
            else if(singleStr.matches("^[0-9]+$")){
                tokenType = "num";
            }
            else {
                throw new Exception("Error: Invalid token, " + singleStr);
            }

            //adding token to the tree
            if (tree == null) {
                tree = new WhereTree(singleStr, tokenType);
            }

            //traversing tree
            else{
                WhereTree newNode = new WhereTree(singleStr, tokenType);
                
                if (tokenType.equals("RelOp")) {//if a relational operator appears, create a subtree
                    
                    if (tree != null && tree.tokenType.equals("colName")) {
                        newNode.leftChild = tree; //set column name as left child
                        tree = newNode; //make the operator the new root
                    } else {
                        throw new Exception("Syntax error: Relational operator must follow a column name.");
                    }
                } 

                else if (tokenType.equals("num") || tokenType.equals("Str") || tokenType.equals("T/F") || tokenType.equals("colName")) {
                    
                    //if it's a value or column name, it could be the right-hand side of a relational expression
                    if (tree != null && tree.tokenType.equals("RelOp")) {
                        tree.rightChild = newNode; //attach as the right child of the relational operator
                    } else {
                        tree = newNode; //else, treat it as the root (until an operator appears)
                    }
                } 
                else if (tokenType.equals("And/Or")) {//if And/Or appears, it needs to be structured correctly
            
                    //esure precedence: AND binds stronger than OR
                    if (singleStr.equals("or") || (singleStr.equals("and") && tree.tokenType.equals("or"))) {
                        
                        //OR or AND appearing after OR → make it the new root
                        newNode.leftChild = tree;
                        tree = newNode;
                    } else {
                        
                        //AND appearing before OR → attach it lower in the tree
                        newNode.leftChild = tree.rightChild;
                        tree.rightChild = newNode;
                    }
                }


            }
        }
        catch(Exception e){
            System.err.println(e.getMessage());
            e.printStackTrace();
        }

    }

    // Function to print inorder traversal
    public static void printTree(WhereTree node)
    {
        if (node == null)
            return;

        // First recur on left subtree
        printTree(node.leftChild);

        // Now deal with the node
        System.out.print(node.token + " ");

        // Then recur on right subtree
        printTree(node.rightChild);
    }
}
