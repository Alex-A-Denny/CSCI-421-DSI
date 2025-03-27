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
    private String varType;
    private WhereTree leftChild;
    private WhereTree rightChild;

    public WhereTree(String tok, String var,
            WhereTree lChild, WhereTree rChild) {
        this.token = tok;
        this.varType = var;
        this.leftChild = lChild;
        this.rightChild = rChild;
    }

    public String getToken() {
        return token;
    }

    public String getVarType() {
        return varType;
    }

    public WhereTree getLeftChild() {
        return leftChild;
    }

    public WhereTree getRightChild() {
        return rightChild;
    }

}
