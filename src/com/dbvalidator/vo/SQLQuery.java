
package com.dbvalidator.vo;

import java.sql.PreparedStatement;

/**
 *
 * @author AJ
 */
public class SQLQuery {

	String oracleQuery;
	String hanaQuery;
	String oracleFilePath;
	String hanaFilePath;
	PreparedStatement oraclePS;
	PreparedStatement hanaPS;

	public String getOracleQuery() {
		return oracleQuery;
	}

	public void setOracleQuery(final String oracleQuery) {
		this.oracleQuery = oracleQuery;
	}

	public String getHanaQuery() {
		return hanaQuery;
	}

	public void setHanaQuery(final String hanaQuery) {
		this.hanaQuery = hanaQuery;
	}

	public String getOracleFilePath() {
		return oracleFilePath;
	}

	public void setOracleFilePath(final String oracleFilePath) {
		this.oracleFilePath = oracleFilePath;
	}

	public String getHanaFilePath() {
		return hanaFilePath;
	}

	public void setHanaFilePath(final String hanaFilePath) {
		this.hanaFilePath = hanaFilePath;
	}

	public PreparedStatement getOraclePS() {
		return oraclePS;
	}

	public void setOraclePS(final PreparedStatement oraclePS) {
		this.oraclePS = oraclePS;
	}

	public PreparedStatement getHanaPS() {
		return hanaPS;
	}

	public void setHanaPS(final PreparedStatement hanaPS) {
		this.hanaPS = hanaPS;
	}

	@Override
	public String toString() {
		final String msg = "Oracle Query: " + getOracleQuery() + "**** Hana Query: " + getHanaQuery();
		return msg;
	}

}
