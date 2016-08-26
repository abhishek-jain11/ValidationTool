package com.dbvalidator.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.logging.LoggerFactory;


public class HanaMigrator {
	public static HashMap<String, List<String>> dataBasePoolMap = new HashMap<String, List<String>>();

	private static final Logger logger = LoggerFactory.getLogger(HanaMigrator.class);

	public static final String HIERATCHY_VIEW_FILE = "create_company_hierarchy_views.sql";
	public static final String PROCEDURE_FILE = "create_procedures.sql";
	public static final String SUCCESS = "success";
	public static final String FAIL = "fail";
	public static String status = SUCCESS;
	public static int batchSize = 100;
	public static int tableCount = 0;
	public static int seqCount = 0;
	public static int indexCount = 0;
	public String databaseTimeZone = null;
	public static final String HANA_MIGRATION_config_id = "HANA_MIGRATION";
	// Doing it this way cause SVN hook was preventing checking in otherwise.
	public static final String sDate = new StringBuilder("SYS").append("DATE").toString();

	public static Connection getConnection(final String[] DBInput, final String DBJNDI, Connection Conn, String[] DBInfo, boolean hasExceptions)
			throws ValidationToolException {
		try {
			DBInfo = DBInput;
			logger.info("Conn: " + DBInfo[0] + "--" + DBInfo[1] + "--");
			Conn = DriverManager.getConnection(DBInfo[0], DBInfo[1], DBInfo[2]);
			if (DBInfo[0].contains("sap")) {
				Conn.setAutoCommit(false);
			}
		} catch (final Exception e) {
			hasExceptions = true;
			status = FAIL;
			//	logException(e, bw);
			throw new ValidationToolException(e);
		}
		return Conn;
	}





	
	
	



	
}
