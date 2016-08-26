
package com.dbvalidator.vo;

import java.sql.Connection;
import java.util.Map;

/**
 *
 * @author AJ
 */
public class Validation {

	String name;
	String type;
	Table table;
	Map<String, Table> tables;
	Connection oracleConnection;
	Connection hanaConnection;
	SQLQuery sqlQuery;
	boolean verifyAllTables = false;

	public boolean isVerifyAllTables() {
		return verifyAllTables;
	}

	public void setVerifyAllTables(final boolean verifyAllTables) {
		this.verifyAllTables = verifyAllTables;
	}

	public Map<String, Table> getTables() {
		return tables;
	}

	public void setTables(final Map<String, Table> tables) {
		this.tables = tables;
	}

	public Connection getOracleConnection() {
		return oracleConnection;
	}

	public void setOracleConnection(final Connection oracleConnection) {
		this.oracleConnection = oracleConnection;
	}

	public Connection getHanaConnection() {
		return hanaConnection;
	}

	public void setHanaConnection(final Connection hanaConnection) {
		this.hanaConnection = hanaConnection;
	}

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(final String type) {
		this.type = type;
	}

	public Table getTable() {
		return table;
	}

	public SQLQuery getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(final SQLQuery sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public void setTable(final Table table) {
		this.table = table;
	}

	public SQLQuery getSQLQuery() {
		return sqlQuery;
	}

	public void setSQLQuery(final SQLQuery sqlQuery) {
		this.sqlQuery = sqlQuery;
	}


}
