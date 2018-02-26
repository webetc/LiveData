package com.github.webetc.livedata;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class LiveTransactionDatabase extends LiveDatabase {
    private List<LiveResponse> modifications = null;
    private List<LiveResponse> inserts = null;
    private List<LiveResponse> deletes = null;


    public LiveTransactionDatabase() {
        super();
    }


    protected void startTransaction() {
        modifications = new ArrayList<>();
        inserts = new ArrayList<>();
        deletes = new ArrayList<>();
    }


    protected void endTransaction(boolean commit) {
        if (!commit) {
            modifications = null;
            inserts = null;
            deletes = null;
            return;
        }

        List<LiveResponse> responses = new ArrayList<>();

        responses.addAll(modifications);

        // Collect inserts
        for (LiveResponse i : inserts) {
            responses.add(getInserted(i.getSchema(), i.getTable()));
        }

        responses.addAll(deletes);

        add(LiveEvent.create(responses));
    }


    protected void processTransactionSQL(String schema, String sql) {
        try {
            String sqlLower = sql.substring(0, Math.min(100, sql.length() - 1)).toLowerCase();
            boolean good = false;
            Iterator<LiveTable> iLiveTable = getTables();
            while (iLiveTable.hasNext()) {
                LiveTable l = iLiveTable.next();
                if (schema.toLowerCase().equals(l.getSchemaName())
                        && sqlLower.startsWith("update " + l.getTableName()))
                    good = true;
                else if (schema.toLowerCase().equals(l.getSchemaName())
                        && sqlLower.startsWith("insert into " + l.getTableName()))
                    good = true;
                else if (schema.toLowerCase().equals(l.getSchemaName())
                        && sqlLower.startsWith("delete from " + l.getTableName()))
                    good = true;
            }

            // Avoid parsing and checking things not being watched
            if (good) {
                net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(sql);
                if (Update.class.isInstance(stmt)) {
                    handleUpdate(schema, (Update) stmt);
                } else if (Insert.class.isInstance(stmt)) {
                    handleInsert(schema, (Insert) stmt);
                } else if (Delete.class.isInstance(stmt)) {
                    handleDeletes(schema, (Delete) stmt);
                }
            }
        } catch (Exception e) {
            System.err.println("Exception:\n" + sql + "\n" + e.getMessage());
            e.printStackTrace();
        } catch (Error er) {
            System.err.println("ERROR:\n" + sql + "\n" + er.getMessage());
        }
    }


    private void handleUpdate(String schema, Update updateStmt) {
        String tableName = updateStmt.getTable().getName();
        LiveResponse response = new LiveResponse(LiveResponse.Modify, schema, tableName);
        List<String> row = new ArrayList<>();

        // ID
        Expression where = updateStmt.getWhere();
        if (EqualsTo.class.isInstance(where)) {
            EqualsTo equalsTo = (EqualsTo) where;
            Expression left = equalsTo.getLeftExpression();
            Expression right = equalsTo.getRightExpression();
            if (Column.class.isInstance(left)) {
                Column column = (Column) left;
                response.addColumn(column.getColumnName());
                row.add(getExpressionValue(right));

                // other columns
                List<Column> columns = updateStmt.getColumns();
                for (Column c : columns) {
                    response.addColumn(c.getColumnName());
                }

                // other values
                List<Expression> expressions = updateStmt.getExpressions();
                for (Expression e : expressions) {
                    row.add(getExpressionValue(e));
                }

                response.addRecord(row);

//                compress(modifications, response);
                modifications.add(response);
            }
        }
    }


    private void handleInsert(String schema, Insert insertStmt) {
        String tableName = insertStmt.getTable().getName();
        LiveResponse response = new LiveResponse(LiveResponse.Modify, schema, tableName);

        compress(inserts, response);
    }


    private void handleDeletes(String schema, Delete deleteStmt) {
        String tableName = deleteStmt.getTable().getName();
        LiveResponse response = new LiveResponse(LiveResponse.Delete, schema, tableName);
        List<String> row = new ArrayList<>();

        Expression where = deleteStmt.getWhere();
        if (EqualsTo.class.isInstance(where)) {
            EqualsTo equalsTo = (EqualsTo) where;
            Expression left = equalsTo.getLeftExpression();
            Expression right = equalsTo.getRightExpression();
            if (Column.class.isInstance(left)) {
                Column column = (Column) left;
                response.addColumn(column.getColumnName());
                row.add(getExpressionValue(right));
                response.addRecord(row);

                compress(deletes, response);
            }
        }
    }


    private void compress(List<LiveResponse> responses, LiveResponse response) {
        for (LiveResponse lr : responses) {
            if (lr.getSchema().equals(response.getSchema())
                    && lr.getTable().equals(response.getTable())) {
                if (lr.getRecords() != null || response.getRecords() != null) {
                    if (lr.getRecords() == null)
                        lr.setRecords(new ArrayList<>());
                    if (response.getRecords() == null)
                        return;
                    lr.getRecords().addAll(response.getRecords());
                }
                return;
            }
        }

        // No match so just add
        responses.add(response);
    }


    private String getExpressionValue(Expression e) {
        if (StringValue.class.isInstance(e)) {
            return (((StringValue) e).getValue());
        } else if (LongValue.class.isInstance(e)) {
            return (String.valueOf(((LongValue) e).getValue()));
        } else if (DoubleValue.class.isInstance(e)) {
            return (String.valueOf(((DoubleValue) e).getValue()));
        } else if (DateValue.class.isInstance(e)) {
            return (String.valueOf(((DateValue) e).getValue()));
        } else if (TimestampValue.class.isInstance(e)) {
            return (String.valueOf(((TimestampValue) e).getValue()));
        }

        return null;
    }
}
