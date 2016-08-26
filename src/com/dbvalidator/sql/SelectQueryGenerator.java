
package com.dbvalidator.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dbvalidator.vo.Column;
import com.dbvalidator.vo.SQLQuery;
import com.dbvalidator.vo.Table;
import com.dbvalidator.vo.Verify;

/**
 *
 * @author AJ
 */
public class SelectQueryGenerator {
	static String rowCountSql = "Select count(*) as records  ";

	public static SQLQuery generateTableRowCountQuery(final String tableName, final String targetSchemaName, final String sourceSchemaName) {
		final StringBuilder sb = new StringBuilder();

		sb.append(rowCountSql);
		sb.append(" , '");
		sb.append(tableName);
		sb.append("' as table_name from ");
		final SQLQuery sql = new SQLQuery();
		sql.setHanaQuery(sb.toString() + targetSchemaName + "." + tableName);
		sql.setOracleQuery(sb.toString() + sourceSchemaName + "." + tableName);

		return sql;
	}

	public static List<SQLQuery> generateBatchedSelectQuery(final Table table, final String targetSchemaName, final Long noOfRecords, final int batchSize) {
		final List<SQLQuery> sqlList = new ArrayList<SQLQuery>();
		long noOfBatches = noOfRecords.longValue() / batchSize;
		if (noOfRecords % batchSize != 0) {
			noOfBatches += 1;
		}
		long start = 1;
		long end = batchSize;
		for (int i = 0; i < noOfBatches; i++) {
			final SQLQuery sql = generateSelectQuery(table, targetSchemaName, start, end);
			start = end + 1;
			end += batchSize;
			sqlList.add(sql);
		}

		return sqlList;
	}

	public static SQLQuery generateSelectQuery(final Table table, final String targetSchemaName, final long start, final long end) {
		final SQLQuery sql = new SQLQuery();
		final String oracleQuery = generateOracleSelectQuery(table, start, end);
		final String hanaQuery = generateHanaSelectQuery(table, targetSchemaName, start, end);
		sql.setOracleQuery(oracleQuery);
		sql.setHanaQuery(hanaQuery);
		return sql;
	}

	public static SQLQuery generateSelectQuery(final Table table, final String targetSchemaName) {
		final SQLQuery sql = new SQLQuery();
		final String oracleQuery = generateOracleSelectQuery(table);
		final String hanaQuery = generateHanaSelectQuery(table, targetSchemaName);
		sql.setOracleQuery(oracleQuery);
		sql.setHanaQuery(hanaQuery);
		return sql;
	}

	public static String generateOracleSelectQuery(final Table table, final long start, final long end) {
		String orderByColumns = table.getPrimaryKeys();
		if (table.getPrimaryKeys() == null || table.getPrimaryKeys().isEmpty()) {
			String delim = "";
			final String delim2 = " , ";
			for (final Entry<String, Column> col : table.getColumns().entrySet()) {
				final Column colData = col.getValue();
				final String ColumnName = col.getKey();
				final String colDataType = colData.getDataType();

				if (!colDataType.contains("LOB")) {
					orderByColumns += delim + ColumnName;
					delim = delim2;
				}
			}
		}
		String sql = "";
		if (table.getVerify() == Verify.ALL) {
			sql = "Select * from (Select row_number() over (order by " + orderByColumns + " ) as rn, tab.* from ";
			sql += " (" + generateOracleSelectQuery(table) + ") tab ) ";
			sql += "where rn between " + start + " and " + end;

			if (table.getName().equals("PA_APP_ADMIN")) {
				sql += " and app_admin_id not in ('HANA_MIGRATION')";
			}
		} else {
			sql = generateOracleSelectQuery(table);
		}
		return sql;
	}

	public static String generateHanaSelectQuery(final Table table, final String targetSchemaName, final long start, final long end) {

		String sql = "";
		if (table.getVerify() == Verify.ALL) {
			sql = generateOracleSelectQuery(table, start, end);

		} else {
			sql = generateHanaSelectQuery(table, targetSchemaName);
		}
		sql = sql.replace(table.getName(), targetSchemaName + "." + table.getName());

		return sql;
	}

	public static String generateOracleSelectQuery(final Table table) {
		String sql = generateSelectQuery(table);
		String orderByColumns = table.getPrimaryKeys();
		if (table.getPrimaryKeys() == null || table.getPrimaryKeys().isEmpty()) {
			String delim = "";
			final String delim2 = " , ";
			for (final Entry<String, Column> col : table.getColumns().entrySet()) {
				final Column colData = col.getValue();
				final String ColumnName = col.getKey();
				final String colDataType = colData.getDataType();

				if (!colDataType.contains("LOB")) {
					orderByColumns += delim + ColumnName;
					delim = delim2;
				}
			}
		}

		if (table.getVerify() == Verify.FIRSTNLAST) {
			sql += generateOrderByClause(orderByColumns, Verify.FIRST);
			sql = "select * from ( ( " + sql + " )a ) ";
			sql += generateOracleLimitClause(Verify.FIRST);
			sql += " UNION ALL ";
			String sql2 = generateSelectQuery(table);
			sql2 += generateOrderByClause(orderByColumns, Verify.LAST);
			sql2 = "select * from ( ( " + sql2 + " )b ) ";
			sql2 += generateOracleLimitClause(Verify.FIRST);
			sql += sql2;
		} else {
			sql += generateOrderByClause(table.getPrimaryKeys(), table.getVerify());
		}

		return sql;
	}

	public static String generateHanaSelectQuery(final Table table, final String targetSchema) {
		String sql = generateSelectQuery(table);
		//Appednd targetSchema to tableName
		sql = sql.replace(table.getName(), targetSchema + "." + table.getName());
		String orderByColumns = table.getPrimaryKeys();
		if (table.getPrimaryKeys() == null || table.getPrimaryKeys().isEmpty()) {
			String delim = "";
			final String delim2 = " , ";
			for (final Entry<String, Column> col : table.getColumns().entrySet()) {
				final Column colData = col.getValue();
				final String ColumnName = col.getKey();
				final String colDataType = colData.getDataType();

				if (!colDataType.contains("LOB")) {
					orderByColumns += delim + ColumnName;
					delim = delim2;
				}
			}
		}
		if (table.getVerify() == Verify.FIRSTNLAST) {

			sql += generateOrderByClause(orderByColumns, Verify.FIRST);
			sql += generateHanaLimitClause(Verify.FIRST);
			sql = "select * from ( " + sql + " )a  ";
			sql += " UNION ALL ";
			String sql2 = generateSelectQuery(table);
			sql2 += generateOrderByClause(orderByColumns, Verify.LAST);
			sql2 += generateHanaLimitClause(Verify.LAST);
			sql2 = "select * from ( " + sql2 + " )b  ";
			sql += sql2;
		} else {
			sql += generateOrderByClause(orderByColumns, table.getVerify());
		}
		return sql;
	}

	private static String generateSelectQuery(final Table table) {
		String query = "";
		if (table != null && table.getName() != null && table.getName().length() > 0) {
			final String delim = ",";
			String loopDelim = "";
			final Map<String, Column> columns = table.getColumns();
			String columnList = "";
			for (final Entry<String, Column> column : columns.entrySet()) {
	
				columnList += loopDelim + "\"" + column.getKey() + "\"";
				loopDelim = delim;

			}
			if (columnList != null && columnList.length() > 0) {
				query = "Select " + columnList + " from " + table.getName();
			}

			if (table.getName().equals("PA_APP_ADMIN")) {
				query += " where app_admin_id not in ('HANA_MIGRATION')";
			}

		}
		return query;
	}

	private static String generateSelectQuery(final Column column, final Table table, final String schemaName) {
		String query = "";
		if (table != null && table.getName() != null && table.getName().length() > 0) {

			final Verify mode = column.getVerify();
			final String columnName = new String("\"" + column.getName() + "\"");
			String columnNames = "";
			final List<String> columns = Arrays.asList(table.getPrimaryKeys().split(","));

			switch (mode) {
				case MAX:
					columnNames = "max(" + columnName + "\")";
					break;
				case MIN:
					columnNames = "min(" + columnName + ")";
					break;
				case AVG:
					columnNames = "avg(" + columnName + ")";
					break;
				default:
					if (columns.contains(column.getName())) {
						columnNames = (table.getPrimaryKeys() != null && table.getPrimaryKeys().length() > 0) ? (table.getPrimaryKeys()) : "";
					} else {
						columnNames = columnName + ", "
								+ ((table.getPrimaryKeys() != null && table.getPrimaryKeys().length() > 0) ? (table.getPrimaryKeys()) : "");
					}

			}

			if (column != null && column.getName().length() > 0) {

				query = " Select " + columnNames + " from " + schemaName + "." + table.getName();

			}
		}
		return query;
	}

	private static String generateOrderByClause(final String primaryKeys, final Verify verify) {
		String orderBy = "";
		if (primaryKeys != null && primaryKeys.length() > 0) {
			orderBy = " order by " + primaryKeys;
			switch (verify) {
				case FIRST:
					break;
				case LAST:
					orderBy += " DESC ";
					break;
				case AVG:
					orderBy = "";
					break;
				case ALL:
					break;
			}
		}
		return orderBy;
	}

	public static String generateOracleOrderByClause(final String primaryKeys, final Verify verify) {
		String orderBy = generateOracleLimitClause(verify);
		orderBy += generateOrderByClause(primaryKeys, verify);
		return orderBy;
	}

	public static String generateHanaOrderByClause(final String primaryKeys, final Verify verify) {
		String orderBy = generateOrderByClause(primaryKeys, verify);
		if (primaryKeys != null && primaryKeys.length() > 0) {
			switch (verify) {
				case FIRST:
					orderBy += " NULLS LAST ";
					break;
				case LAST:
					orderBy += " NULLS FIRST ";
					break;
				case ALL:
					break;
			}
		}
		orderBy += generateHanaLimitClause(verify);
		return orderBy;

	}

	private static String generateLimitClause(final Verify verify) {
		final String limitClause = "";
		return limitClause;
	}

	public static String generateOracleLimitClause(final Verify verify) {
		final String limitClause = generateLimitClause(verify);
		String oracleLimitClause = "";
		switch (verify) {
			case FIRST:
				oracleLimitClause += " where ROWNUM = 1  ";
				break;
			case LAST:
				oracleLimitClause += " where ROWNUM = 1 ";
				break;
			case ALL:
				break;
		}
		oracleLimitClause += limitClause;
		return oracleLimitClause;

	}

	public static String generateHanaLimitClause(final Verify verify) {
		final String limitClause = generateLimitClause(verify);
		String hanaLimitClause = "";
		switch (verify) {
			case FIRST:
				hanaLimitClause += " LIMIT 1 ";
				break;
			case LAST:
				hanaLimitClause += " LIMIT 1 ";
				break;
		}
		hanaLimitClause += limitClause;
		return hanaLimitClause;
	}

	public static String generateHanaSelectQuery(final Table table, final Column column) {
		String query = generateSelectQuery(column, table, table.getTargetSchemaName());
		String orderByColumns = table.getPrimaryKeys();
		if (table.getPrimaryKeys() == null || table.getPrimaryKeys().isEmpty()) {
			String delim = "";
			final String delim2 = " , ";
			for (final Entry<String, Column> col : table.getColumns().entrySet()) {
				final Column colData = col.getValue();
				final String ColumnName = col.getKey();
				final String colDataType = colData.getDataType();

				if (!colDataType.contains("LOB")) {
					orderByColumns += delim + "\"" + ColumnName + "\"";
					delim = delim2;
				}
			}
		}
		if (column.getVerify() == Verify.FIRSTNLAST) {
			query += generateHanaOrderByClause(orderByColumns, Verify.FIRST);

			query = "select * from ( " + query + " )a  ";
			query += " UNION ALL ";
			String sql2 = generateSelectQuery(column, table, table.getTargetSchemaName());
			sql2 += generateHanaOrderByClause(orderByColumns, Verify.LAST);

			sql2 = "select * from ( " + sql2 + " )b  ";
			query += sql2;
		} else {
			query += generateHanaOrderByClause(orderByColumns, column.getVerify());
		}

		return query;
	}

	public static String generateOracleSelectQuery(final Table table, final Column column) {
		String query = generateSelectQuery(column, table, table.getSourceSchemaName());
		String orderByColumns = table.getPrimaryKeys();
		if (table.getPrimaryKeys() == null || table.getPrimaryKeys().isEmpty()) {
			String delim = "";
			final String delim2 = " , ";
			for (final Entry<String, Column> col : table.getColumns().entrySet()) {
				final Column colData = col.getValue();
				final String ColumnName = col.getKey();
				final String colDataType = colData.getDataType();

				if (!colDataType.contains("LOB")) {
					orderByColumns += delim + ColumnName;
					delim = delim2;
				}
			}
		}

		if (column.getVerify() == Verify.FIRSTNLAST) {
			query += generateOrderByClause(orderByColumns, Verify.FIRST);
			query = "select * from ( ( " + query + " )a ) ";
			query += generateOracleLimitClause(Verify.FIRST);
			query += " UNION ALL ";
			String sql2 = generateSelectQuery(column, table, table.getSourceSchemaName());
			sql2 += generateOrderByClause(orderByColumns, Verify.LAST);
			sql2 = "select * from ( ( " + sql2 + " )b ) ";
			sql2 += generateOracleLimitClause(Verify.FIRST);
			query += sql2;
		} else {
			query += generateOracleOrderByClause(orderByColumns, column.getVerify());
		}
		return query;
	}

	public static SQLQuery generateSQLQuery(final Table table, final Column column) {
		final SQLQuery sql = new SQLQuery();
		final String hanaQuery = generateHanaSelectQuery(table, column);
		final String oracleQuery = generateOracleSelectQuery(table, column);
		sql.setHanaQuery(hanaQuery);
		sql.setOracleQuery(oracleQuery);
		return sql;
	}

	public static List<SQLQuery> generateBatchedSelectQuery(final Table table, final Column column, final String targetSchemaName, final Long noOfRecords,
			final int batchSize) {
		final List<SQLQuery> sqlList = new ArrayList<SQLQuery>();
		long noOfBatches = noOfRecords.longValue() / batchSize;
		if (noOfRecords % batchSize != 0) {
			noOfBatches += 1;
		}
		long start = 1;
		long end = batchSize;
		for (int i = 0; i < noOfBatches; i++) {
			final SQLQuery sql = generateSelectQuery(table, column, targetSchemaName, start, end);
			start = end + 1;
			end += batchSize;
			sqlList.add(sql);
		}

		return sqlList;
	}

	public static SQLQuery generateSelectQuery(final Table table, final Column column, final String targetSchemaName, final long start, final long end) {
		final SQLQuery sql = new SQLQuery();
		final String oracleQuery = generateOracleSelectQuery(table, column, start, end);
		final String hanaQuery = generateHanaSelectQuery(table, column, targetSchemaName, start, end);
		sql.setOracleQuery(oracleQuery);
		sql.setHanaQuery(hanaQuery);
		return sql;
	}

	public static String generateOracleSelectQuery(final Table table, final Column column, final long start, final long end) {
		final String orderByColumns = table.getPrimaryKeys();
		String sql = "";
		if (column.getVerify() == Verify.ALL) {
			sql = "Select * from (Select row_number() over (order by " + orderByColumns + " ) as rn, tab.* from ";
			sql += " (" + generateOracleSelectQuery(table, column) + ") tab ) ";
			sql += "where rn between " + start + " and " + end;
		} else {
			sql = generateOracleSelectQuery(table, column);
		}
		return sql;
	}

	public static String generateHanaSelectQuery(final Table table, final Column column, final String targetSchemaName, final long start, final long end) {

		String sql = "";
		if (column.getVerify() == Verify.ALL) {
			sql = generateHanaSelectQuery(table, column, start, end);

		} else {
			sql = generateHanaSelectQuery(table, column);
		}
		//	sql = sql.replace(table.getName(), targetSchemaName + "." + table.getName());

		return sql;
	}

	public static String generateHanaSelectQuery(final Table table, final Column column, final long start, final long end) {
		String sql = "";
		if (column.getVerify() == Verify.ALL) {
			sql = generateHanaSelectQuery(table, column) + " LIMIT " + (end - start + 1) + " OFFSET " + (start - 1);
		} else {
			sql = generateOracleSelectQuery(table, column);
		}
		return sql;
	}

}
