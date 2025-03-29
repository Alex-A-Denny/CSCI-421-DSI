package clauses;

import page.RecordEntry;
import table.TableSchema;

import java.util.ArrayDeque;
import java.util.List;

@SuppressWarnings("unchecked")
public class WhereEvaluator {
    private final ArrayDeque<List<Object>> tree;

    public WhereEvaluator(ArrayDeque<List<Object>> tree) {
        this.tree = tree;
    }

    public boolean evaluate(RecordEntry record, TableSchema schema) {
        var root = tree.getFirst();
        try {
            return evaluate(record, schema, root);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return false;
        }
    }

    private static boolean evaluate(RecordEntry record, TableSchema schema, List<Object> node) throws Exception {
        Object operatorRaw = node.get(0);
        List<Object> operatorList = (List<Object>) operatorRaw;
        Token operator = (Token) operatorList.get(0);
        if (operator.type.equals("RelOp")) {
            Token left = (Token) ((List<Object>) node.get(1)).get(0);
            Token right = (Token) ((List<Object>) node.get(2)).get(0);

            String column = left.value;
            int colIndex = schema.getColumnIndex(column);
            if (colIndex < 0) {
                System.err.println("Error: Column not found: " + column);
                return false;
            }

            Object leftVal = record.data.get(colIndex);
            if (leftVal == null) {
                return false;
            }
            Object rightVal = parseValue(right.value, right.type);
            if (rightVal == null) {
                return false;
            }
            int cmp = compare(leftVal, rightVal);
            return switch (operator.value) {
                case "=" -> cmp == 0;
                case "!=" -> cmp != 0;
                case ">" -> cmp > 0;
                case "<" -> cmp < 0;
                case ">=" -> cmp >= 0;
                case "<=" -> cmp <= 0;
                default -> false;
            };
        } else if (operator.type.equals("And/Or")) {
            List<Object> left = (List<Object>) node.get(1);
            boolean leftResult = evaluate(record, schema, left);
            List<Object> right = (List<Object>) node.get(2);
            boolean rightResult = evaluate(record, schema, right);
            if (operator.value.equals("and")) {
                return leftResult && rightResult;
            }
            return leftResult || rightResult;
        } else {
            System.err.println("Error: Non operator token found");
            return false;
        }
    }

    private static Object parseValue(String token, String type) {
        try {
            return switch (type) {
                case "num" -> {
                    if (token.contains(".")) {
                        yield Double.parseDouble(token);
                    }
                    yield Integer.parseInt(token);
                }
                case "T/F" -> Boolean.parseBoolean(token);
                case "Str" -> token.replace("\"", "");
                case "colName" -> token;
                default -> token;
            };
        } catch (Exception e) {
            return null;
        }
    }

    private static int compare(Object a, Object b) throws Exception {
        if (a instanceof Integer intA) {
            if (b instanceof Integer intB) {
                return intA.compareTo(intB);
            }
            throw new Exception("Cannot compare int to " + b.getClass().getSimpleName());
        }
        if (a instanceof Double doubleA) {
            if (b instanceof Double doubleB) {
                return doubleA.compareTo(doubleB);
            }
            throw new Exception("Cannot compare double to " + b.getClass().getSimpleName());
        }
        if (a instanceof Boolean boolA) {
            if (b instanceof Boolean boolB) {
                return boolA.compareTo(boolB);
            }
            throw new Exception("Cannot compare boolean to " + b.getClass().getSimpleName());
        }
        if (a instanceof String strA) {
            if (b instanceof String strB) {
                return strA.compareTo(strB);
            }
            throw new Exception("Cannot compare String to " + b.getClass().getSimpleName());
        }
        throw new Exception("Table type was invalid");
    }
}
