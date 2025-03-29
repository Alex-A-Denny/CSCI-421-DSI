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

import java.util.ArrayDeque;
import java.util.List;
import page.RecordEntry;
import table.Table;
import table.TableSchema;

public class WhereClause {

    static WhereTree conditionalTree;

    public boolean passesConditional(){
        return true;
    }

    private static int operatorPriority(List<Object> operator) {
        Object o = operator.get(0);
        while (o instanceof List<?> list) {
            o = list.get(0);
        }
        if (o instanceof Token token) {
            if (token.type.equals("RelOp")) {
                return 1;
            }
            if (token.type.equals("And/Or")) {
                if (token.value.equals("and")) {
                    return 2;
                }
                return 3;
            }
        }
        return -1;
    }

    public static WhereEvaluator parseWhere(String userInput, List<Table> tables) {
        try {
            WhereTree.conditionalTreeRoot = null;

            List<Token> tokenList = WhereTree.tokenize(userInput, tables);

            ArrayDeque<List<Object>> operators = new ArrayDeque<>();
            ArrayDeque<List<Object>> operands = new ArrayDeque<>();
            for (Token t : tokenList) {
                if (t.type.equals("RelOp") || t.type.equals("And/Or")) {
                    if (operators.isEmpty()) {
                        operators.addLast(List.of(t));
                    } else if (operatorPriority(operators.getLast()) < operatorPriority(List.of(t))) {
                        var right = operands.removeLast();
                        var left = operands.removeLast();
                        operands.addLast(List.of(operators.removeLast(), left, right));
                        operators.addLast(List.of(t));
                    } else {
                        operators.addLast(List.of(t));
                    }
                } else {
                    operands.addLast(List.of(t));
                }
            }
            while (!operators.isEmpty()) {
                var op = operators.removeLast();
                var right = operands.removeLast();
                var left = operands.removeLast();
                operands.addLast(List.of(op, left, right));
            }
            return new WhereEvaluator(operands);

//            for (Token t : tokenList) {
//                WhereTree.buildTree(t);
//            }
//
//            conditionalTree = WhereTree.conditionalTreeRoot;
//            return true;
        } catch (Exception e) {
            System.err.println("Error parsing WHERE clause: " + e.getMessage());
            e.printStackTrace(); // TODO remove
            return null;
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
