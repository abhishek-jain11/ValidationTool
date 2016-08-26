/* -----------------------------------------------------------------------------
 * Copyright (c) 2013 SuccessFactors, all rights reserved.
 *
 * This software and documentation is the confidential and proprietary
 * information of SuccessFactors.  SuccessFactors makes no representation
 * or warranties about the suitability of the software, either expressed or
 * implied.  It is subject to change without notice.
 *
 * U.S. and international copyright laws protect this material.  No part
 * of this material may be reproduced, published, disclosed, or
 * transmitted in any form or by any means, in whole or in part, without
 * the prior written permission of SuccessFactors.
 * -----------------------------------------------------------------------------
 */

package com.dbvalidator.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.logging.LoggerFactory;
import com.dbvalidator.sql.QueryExecutor;
import com.dbvalidator.sql.SelectQueryGenerator;
import com.dbvalidator.validators.Validator;
import com.dbvalidator.vo.Column;
import com.dbvalidator.vo.SQLQuery;
import com.dbvalidator.vo.Table;
import com.dbvalidator.vo.Tuple;
import com.dbvalidator.vo.Validation;
import com.dbvalidator.vo.ValidationResultVO;
import com.dbvalidator.vo.Validations;
import com.dbvalidator.vo.Verify;


/**
 *
 * @author AJ
 */
public class HanaMigrationValidator {
	//Check system time
	//Sample records- should have same record in both tables
	Connection targetConnection;
	Connection sourceConnection;
	String targetSchema;
	String sourceSchema;
	TimeZone dbTimezone;

	public TimeZone getDbTimezone() {
		return dbTimezone;
	}

	public void setDbTimezone(final TimeZone dbTimezone) {
		this.dbTimezone = dbTimezone;
	}

	public HanaMigrationValidator(final Connection sourceConnection, final String sourceSchema, final Connection targeConnection, final String targetSchema) {
		this.targetConnection = targeConnection;
		this.sourceConnection = sourceConnection;
		this.sourceSchema = sourceSchema;
		this.targetSchema = targetSchema;
	}

	final Logger logger = LoggerFactory.getLogger(HanaMigrationValidator.class);

	public void startValidationTestSuite() throws ValidationToolException {
		//Read props file	
			Properties validationConfigurations = null;
			try {
				final String fileName = System.getProperty("user.dir") + File.separator + "ValidationTests.properties";
				logger.info("Reading Validation Tests from " + fileName);
				final File file = new File(fileName);
				final InputStream in = new FileInputStream(file);
				validationConfigurations = new Properties();
				//validationConfigurations = new Properties(in);
			} catch (final FileNotFoundException e) {
				logger.warn("ValidationTests.properties not found in File System. Checking in the jar");
				validationConfigurations = new Properties();//Props(ResourceLoaderUtils.getResourceAsStream("ValidationTests.properties"));
			}

					final Validations validations = new Validations(); //Convert this Map<String, List<ValidationTest>>
		//	PropertyConverter.populateObject(validations, validationConfigurations, null);

			final Map<String, Integer> results = new HashMap<String, Integer>();
			for (final Entry<String, Validation> validation : validations.getValidtionTests().entrySet()) {
				final String validationName = validation.getKey();
				final Validation validationTest = validation.getValue();
				final Table table = validationTest.getTable();

				final Map<String, Table> tables = validationTest.getTables();
				logger.info("Started " + validationName);

				if (tables != null) {
					//Get ALL LMS Tables
					try {
						final boolean verifyALLTables = validationTest.isVerifyAllTables();
						final List<String> validateTableList = new ArrayList<String>();
						if (!verifyALLTables) {
							for (final String tableName : tables.keySet()) {
								validateTableList.add(tableName);
							}
						}

						final Map<String, Table> tableColumnsMap = getAllTablesMap(sourceConnection, targetConnection, targetSchema, sourceSchema,
								verifyALLTables, validateTableList);

						//Override Table and column Validators with Properties file
						for (final Entry<String, Table> tableEntry : tables.entrySet()) {
							final String tableName = tableEntry.getKey();
							if (tableColumnsMap.containsKey(tableName)) {
								final Table tab = tableEntry.getValue();
								final Table overrideTable = tableColumnsMap.get(tableName);
								overrideTable.override(tab);
							}
						}

						//Initialize All the validator based on the column Datatype
						final int batchSize = 1000;

						for (final Entry<String, Table> tableEntryMap : tableColumnsMap.entrySet()) {
							LinkedList<Tuple> oracleRecords = null;
							LinkedList<Tuple> hanaRecords = null;
							final Table tableEntry = tableEntryMap.getValue();
							final Map<String, Column> columnMap = tableEntry.getColumns();
							final SQLQuery tableRowCountQuery = SelectQueryGenerator.generateTableRowCountQuery(tableEntry.getName(), targetSchema,
									sourceSchema);

							logger.info("Validating table " + tableEntry.getName());
							final LinkedList<Tuple> oracletableRowCount = QueryExecutor.executeQueryString(sourceConnection,
									tableRowCountQuery.getOracleQuery());
							final Long oracleNoRecords = new Long(((BigDecimal) oracletableRowCount.getFirst().getTuple().get("RECORDS")).intValue());
							
							final Long tableRecords = oracleNoRecords;

							if (tableRecords.longValue() > 0) {
								if (tableEntry.getVerify() == Verify.ALL) {
									final List<SQLQuery> sqlList = SelectQueryGenerator.generateBatchedSelectQuery(tableEntry, targetSchema, tableRecords,
											batchSize);
									final String validationIdentifier = validationName + "." + tableEntry.getName();
									logger.info("Validation Identifier" + validationIdentifier);

									int noOfErrors = 0;
									int batchNumber = 1;
									logger.info("Validating ALL records of table " + tableEntry.getName());
									for (final SQLQuery batchedSQL : sqlList) {
										oracleRecords = QueryExecutor.executeQueryString(sourceConnection, batchedSQL.getOracleQuery());
										hanaRecords = QueryExecutor.executeQueryString(targetConnection, batchedSQL.getHanaQuery());

										int i = 0;

										logger.info("Validating batch " + batchNumber + " started");
										for (final Tuple oracleRecord : oracleRecords) {
											final Tuple hanaRecord = hanaRecords.get(i);

											for (final Entry<String, Column> entry : columnMap.entrySet()) {
												final Column col = entry.getValue();
												//logger.info("Validating ALL records of table: " + tableEntry.getName() + " Column: " + col.getName());

												batchNumber++;

												final Validator migrationvalidator = HanaMigrationValidatorFactory.getValidator(col.getValidator(),
														col.getName(), tableEntry.getName(), dbTimezone);

												final ValidationResultVO result = migrationvalidator.validate(oracleRecord, hanaRecord);
												if (result != null) {
													noOfErrors++;
													//logger.error(result);
												}

											}
											i++;

										}
										logger.info("Validating batch " + batchNumber + " completed");
										batchNumber++;
									}
									results.put(validationIdentifier, Integer.valueOf(noOfErrors));
								} else {
									for (final Entry<String, Column> entry : columnMap.entrySet()) {
										final Column col = entry.getValue();
										final String validationIdentifier = validationName + "." + tableEntry.getName() + "." + col.getName();
										int noOfErrors = 0;
										if (col.getVerify() != Verify.NONE) {
											//Validating by each column

											if (tableEntry.getPrimaryKeys() != null && tableEntry.getPrimaryKeys().length() > 0) {

												//final SQLQuery sql = SelectQueryGenerator.generateSQLQuery(tableEntry, col);
												if (tableRecords > batchSize && col.getVerify() == Verify.ALL) {

													final List<SQLQuery> sqlList = SelectQueryGenerator.generateBatchedSelectQuery(tableEntry, col,
															targetSchema, tableRecords, batchSize);
													int batchNumber = 1;
													logger.info("Validating ALL records of table: " + tableEntry.getName() + " Column: " + col.getName());
													for (final SQLQuery sql : sqlList) {
														//							final long currentTime = Calendar.getInstance().getTime().getTime();
														logger.info("Validating batch " + batchNumber);
														oracleRecords = QueryExecutor.executeQueryString(sourceConnection, sql.getOracleQuery());
														//								final long oracleExecutionTime = Calendar.getInstance().getTime().getTime();
														//									logger.info("Time to get Oracle Results" + (oracleExecutionTime - currentTime));
														hanaRecords = QueryExecutor.executeQueryString(targetConnection, sql.getHanaQuery());
														//								final long hanaExecutionTime = Calendar.getInstance().getTime().getTime();
														//									logger.info("Time to get Hana Results" + (hanaExecutionTime - oracleExecutionTime));

														batchNumber++;
														final Validator migrationvalidator = HanaMigrationValidatorFactory
																.getValidator(col.getValidator(), col.getName(), tableEntry.getName(), dbTimezone);

														logger.info("Executing Validator " + migrationvalidator.getClass().getName());
														noOfErrors += migrationvalidator.validate(oracleRecords, hanaRecords);
																									}
													logger.info(
															"Completed Validating ALL records of table: " + tableEntry.getName() + " Column: " + col.getName());
												} else {
													logger.info("Validating " + col.getVerify().toString() + " records of table: " + tableEntry.getName()
															+ " Column: " + col.getName());
													final SQLQuery sql = SelectQueryGenerator.generateSQLQuery(tableEntry, col);
													oracleRecords = QueryExecutor.executeQueryString(sourceConnection, sql.getOracleQuery());
													hanaRecords = QueryExecutor.executeQueryString(targetConnection, sql.getHanaQuery());

													final Validator migrationvalidator = HanaMigrationValidatorFactory.getValidator(col.getValidator(),
															col.getName(), tableEntry.getName(), dbTimezone);
													logger.info("Executing Validator " + migrationvalidator.getClass().getName());
													noOfErrors += migrationvalidator.validate(oracleRecords, hanaRecords);
													logger.info("Completed Validating " + col.getVerify().toString() + " records of table :"
															+ tableEntry.getName() + " Column :" + col.getName());
												}

											} else {
												logger.warn(
														"Skipping validation of " + tableEntry.getName() + "." + col.getName() + " as no primary key present");
											}
										} else {
											logger.warn("Skipping validation of " + tableEntry.getName() + "." + col.getName()
													+ " as Verify level configured None");
										}
										results.put(validationIdentifier, noOfErrors);
									}
								}

							} else {
								logger.info("No records found in Oracle and Hana.");
							}

						}

					} catch (final SQLException e) {
						throw new ValidationToolException(e);
					}
				} else
				//Tables
				if (table != null) {
					table.setSourceSchemaName(sourceSchema);
					table.setTargetSchemaName(targetSchema);
					final Map<String, Column> columnMap = table.getColumns();

					for (final Entry<String, Column> entry : columnMap.entrySet()) {

						final Column col = entry.getValue();
						final SQLQuery sql = SelectQueryGenerator.generateSQLQuery(table, col);
						final String validationIdentifier = validationName + "." + table.getName() + "." + col.getName();
						int noOfErrors = 0;
						final LinkedList<Tuple> oracleRecords = QueryExecutor.executeQueryString(sourceConnection, sql.getOracleQuery());
						final LinkedList<Tuple> hanaRecords = QueryExecutor.executeQueryString(targetConnection, sql.getHanaQuery());

						final Validator migrationvalidator = HanaMigrationValidatorFactory.getValidator(col.getValidator(), col.getName(),
								table.getName(), dbTimezone);
						logger.info("Executing Validator " + migrationvalidator.getClass().getName());
						noOfErrors += migrationvalidator.validate(oracleRecords, hanaRecords);
						results.put(validationIdentifier, noOfErrors);
					}
				}

			}
			logger.info("*************");
			boolean errorsFound = false;
			logger.info("Results:");
			for (final Entry<String, Integer> validationResult : results.entrySet()) {
				final String validationName = validationResult.getKey();

				final Integer validationResultErrors = validationResult.getValue();
				if (validationResultErrors == null || validationResultErrors.intValue() == 0) {
					logger.debug(validationName + " Passed ");
				} else {
					logger.error(validationName + " Failed Errors found " + validationResultErrors.intValue());

					errorsFound = true;

				}
			}
			if (errorsFound) {
				logger.error("MIGRATION FAILED");
			} else {
				logger.info("MIGRATION SUCCESSFUL");
			}

		
	}

	
	
	private Map<String, Table> getAllTablesMap(final Connection sourceConn, final Connection targetConnection, final String targetSchemaName,
			final String sourceSchemaName, final boolean verifyALLTables, final List<String> tablesList) throws SQLException {
		final List<Table> tableList = getAllTables(sourceConn, targetConnection, targetSchemaName, sourceSchemaName, verifyALLTables, tablesList);
		final Map<String, Table> tableMap = new HashMap<String, Table>();
		for (final Table table : tableList) {
			final String tableName = table.getName();
			tableMap.put(tableName, table);
		}
		return tableMap;
	}

	private List<Table> getAllTables(final Connection sourceConn, final Connection targetConnection, final String targetSchemaName,
			final String sourceSchemaName, final boolean verifyALLTables, final List<String> tablesList) throws SQLException {
		List<Column> tableColumnList = null;
		final List<Table> tableList = new ArrayList<Table>();
		String getTableColumnsSQL = "select table_name,column_name, data_type,data_length,data_precision,data_scale from user_tab_columns where  ";
		if (!verifyALLTables) {
			String tablesListConcat = "";
			String delim = "";
			for (final String tableName : tablesList) {
				tablesListConcat += delim + " '" + tableName + "' ";
				delim = ",";
			}
			getTableColumnsSQL += " table_name in ( " + tablesListConcat + ") ";
		} else {
		
			getTableColumnsSQL += " table_name in (select table_name from user_tables where temporary ='N')";
		}
		getTableColumnsSQL += " order by table_name ";

		//	logger.info(getTableColumnsSQL);
		final PreparedStatement pstmt = sourceConn.prepareStatement(getTableColumnsSQL);
		ResultSet rs = null;
		logger.debug("Fetching columns of table ");
		rs = pstmt.executeQuery();
		tableColumnList = new ArrayList<Column>();
		final Map<String, List<Column>> tableColumnsMap = new HashMap<String, List<Column>>();
		while (rs.next()) {
			final String tableName = rs.getString(1);
			final String columnName = rs.getString(2);
			final String columnDatatype = rs.getString(3);
			final Integer datalength = rs.getInt(4);
			final Integer dataprecision = rs.getInt(5);
			final Integer datascale = rs.getInt(6);
			final Column col = new Column(columnName, columnDatatype, tableName, datalength, dataprecision, datascale);
			if (columnDatatype.equals("NUMBER")) {
				col.setVerify("AVG");
			} else if (columnDatatype.contains("TIMESTAMP") || columnDatatype.contains("DATE")) {
				if (columnName.equals("LST_UPD_TSTMP") || columnName.equals("LAST_UPDATE_TIMESTAMP") || columnName.equals("LAST_UPD_TIMESTAMAP")) {
					col.setVerify("NONE");
				} else {
					col.setVerify("FIRSTNLAST");
				}
			} else {
				col.setVerify("FIRSTNLAST");
			}
			//Add Validator to default datatypes

			final String columnValidator = HanaMigrationValidatorFactory.getDefaultValidatorName(columnDatatype);
			col.setValidator(columnValidator);
			tableColumnList.add(col);
			if (!tableColumnsMap.containsKey(tableName)) {
				final List<Column> tableColumns = new ArrayList<Column>();
				tableColumnsMap.put(tableName, tableColumns);
			}
			final List<Column> tableColumns = tableColumnsMap.get(tableName);
			tableColumns.add(col);
		}
		rs.close();
		pstmt.close();

		//Get ALl Primary Keys
		String sql = "select dcc.table_name,dcc.column_name from user_cons_columns dcc, user_constraints dc where  dcc.constraint_name=dc.constraint_name and dc.constraint_type='P'  ";
		if (!verifyALLTables) {
			String tablesListConcat = "";
			String delim = "";
			for (final String tableName : tablesList) {
				tablesListConcat += delim + " '" + tableName + "' ";
				delim = ",";
			}
			sql += " and dcc.table_name in (" + tablesListConcat + ") ";
		}
		sql += "order by dcc.table_name,position ";
		final PreparedStatement pstmt2 = sourceConnection.prepareStatement(sql);
		logger.debug("Executing: " + sql);
		final ResultSet rs2 = pstmt2.executeQuery();
		final HashMap<String, List<String>> pkmap = new HashMap<String, List<String>>();
		while (rs2.next()) {
			final String table_name = rs2.getString(1);
			final String column_name = rs2.getString(2);
			if (!pkmap.containsKey(table_name)) {
				pkmap.put(table_name, new ArrayList<String>());
			}
			final List<String> cols = pkmap.get(table_name);
			cols.add(column_name);
		}
		pstmt2.close();
		rs2.close();

		//We have all data, now lets construct, the Table object

		for (final Entry<String, List<Column>> tableColumnsEntry : tableColumnsMap.entrySet()) {
			final Table table = new Table();
			final String tableName = tableColumnsEntry.getKey();
			final List<Column> columns = tableColumnsEntry.getValue();
			final Map<String, Column> columnsMap = new HashMap<String, Column>();
			String primaryKeyList = "";
			for (final Column col : columns) {
				final String colName = col.getName();
				columnsMap.put(colName, col);
			}
			List<String> primaryKeys = null;

			if (pkmap.containsKey(tableName)) {
				primaryKeys = pkmap.get(tableName);

				String listDelimiter = "";
				for (final String primaryKey : primaryKeys) {
					primaryKeyList += listDelimiter + primaryKey;
					listDelimiter = ",";
				}

			}

			table.setTargetSchemaName(targetSchemaName);
			table.setSourceSchemaName(sourceSchemaName);
			table.setName(tableName);
			table.setColumns(columnsMap);
			table.setPrimaryKeys(primaryKeyList);
			table.setVerify(Verify.FIRSTNLAST);
			tableList.add(table);

		}
		return tableList;
	}

}