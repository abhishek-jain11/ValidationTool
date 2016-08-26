
package com.dbvalidator.vo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dbvalidator.sql.SelectQueryGenerator;

/**
 *
 * @author AJ
 */
public class Table  {

	String name;
	String targetSchemaName;
	String sourceSchemaName;
	Map<String, Column> columns;
	String primaryKeys;
	Verify verify;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public Map<String, Column> getColumns() {
		return columns;
	}

	public void setColumns(final Map<String, Column> columns) {
		this.columns = columns;
	}

	public String getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(final String primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public Verify getVerify() {
		return verify;
	}

	public void setVerify(final Verify verify) {
		this.verify = verify;
	}

	public String getTargetSchemaName() {
		return targetSchemaName;
	}

	public void setTargetSchemaName(final String targetSchemaName) {
		this.targetSchemaName = targetSchemaName;
	}

	public String getSourceSchemaName() {
		return sourceSchemaName;
	}

	public void setSourceSchemaName(final String sourceSchemaName) {
		this.sourceSchemaName = sourceSchemaName;
	}

	public void override(final Table table) {
		if (table.getVerify() != null) {
			this.verify = table.verify;
		}

		if (table.getColumns() != null && table.getColumns().size() > 0) {
			for (final Entry<String, Column> columnEntry : table.getColumns().entrySet()) {
				final String columnName = columnEntry.getKey();
				final Column columnOverride = columnEntry.getValue();
				final Column col = this.getColumns().get(columnName);
				if (col != null) {
					col.override(columnOverride);
				}
			}
		}

	}

	public List<SQLQuery> getTestQueries() {
		final List<SQLQuery> queryList = new ArrayList<SQLQuery>();
		for (final Entry<String, Column> column : getColumns().entrySet()) {
			final SQLQuery sql = SelectQueryGenerator.generateSQLQuery(this, column.getValue());
			queryList.add(sql);
		}
		return queryList;
	}

}
