//
// WhereClause.java
// Parses where clause, creates AST 
// and determines if a given row 
// meets where requirements
//
// Author: Alex A Denny, Beining Zhou
//
////////////////////////////////////////

package clauses;

import java.util.List;
import page.RecordEntry;
import table.TableSchema;

public class WhereClause {

    static WhereTree conditionalTree;

    public boolean passesConditional(){
        return true;
    }

    public static void parseWhere(String userInput) {
        try {
            WhereTree.conditionalTreeRoot = null;

            List<Token> tokenList = WhereTree.tokenize(userInput);

            for (Token t : tokenList) {
                WhereTree.buildTree(t);
            }

            conditionalTree = WhereTree.conditionalTreeRoot;

        } catch (Exception e) {
            System.err.println("Error parsing WHERE clause: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public static boolean passesConditional(RecordEntry record, TableSchema schema) {
        if (conditionalTree == null) return true;  
        return evaluateTree(conditionalTree, record, schema);
    }

    private static boolean evaluateTree(WhereTree node, RecordEntry record, TableSchema schema) {
        if (node == null) return true;

        switch (node.getTokenType()) {
            case "And/Or" -> {
                boolean left = evaluateTree(node.getLeftChild(), record, schema);
                boolean right = evaluateTree(node.getRightChild(), record, schema);
                return node.getToken().equals("and") ? (left && right) : (left || right);
            }
            case "RelOp" -> {
                WhereTree left = node.getLeftChild();
                WhereTree right = node.getRightChild();
                if (left == null || right == null) return false;

                String column = left.getToken();
                int colIndex = schema.getColumnIndex(column);
                if (colIndex < 0) {
                    System.err.println("Error: Column not found: " + column);
                    return false;
                }

                Object leftVal = record.data.get(colIndex);
                Object rightVal = parseValue(right.getToken(), right.getTokenType());
                if (leftVal == null || rightVal == null) return false;

                int cmp = compare(leftVal, rightVal);
                return switch (node.getToken()) {
                    case "=" -> cmp == 0;
                    case "!=" -> cmp != 0;
                    case ">" -> cmp > 0;
                    case "<" -> cmp < 0;
                    case ">=" -> cmp >= 0;
                    case "<=" -> cmp <= 0;
                    default -> false;
                };
            }
        }

        return true;
    }

    private static Object parseValue(String token, String type) {
        try {
            return switch (type) {
                case "num" -> token.contains(".") ? Double.parseDouble(token) : Integer.parseInt(token);
                case "T/F" -> Boolean.parseBoolean(token);
                case "Str" -> token.replace("\"", "");
                case "colName" -> token;
                default -> token;
            };
        } catch (Exception e) {
            return null;
        }
    }

    private static int compare(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }

        if (a instanceof Comparable && b instanceof Comparable) {
            try {
                return ((Comparable<Object>) a).compareTo(b);
            } catch (ClassCastException e) {
                return String.valueOf(a).compareTo(String.valueOf(b));
            }
        }

        return String.valueOf(a).compareTo(String.valueOf(b));
    }

} 
