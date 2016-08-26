package com.dbvalidator.core;

import java.sql.Connection;

import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.logging.LoggerFactory;

public class CommandLine {
	private static final Logger logger = LoggerFactory.getLogger(CommandLine.class);

	public static void main(String[] args){
		
		}
	
	public void startValidationTest(Connection sourceConn, String sourceSchema,Connection targetConn, String targetSchema){
		logger.info("Starting Data Migration Validation");
		final long startTime = System.currentTimeMillis();
		final HanaMigrationValidator validator = new HanaMigrationValidator(sourceConn, sourceSchema, targetConn, targetSchema);
		try {
			validator.startValidationTestSuite();
		} catch (ValidationToolException e) {
			logger.info("Failed due to: ",e);
		}
		final long endTime = System.currentTimeMillis();
		logger.info("Completed Data Migration Validation");
		logger.info("Time Taken to Complete verification " + (endTime - startTime) / 1000 + " Seconds");
		logger.info("Time Taken to Complete verification " + (((endTime - startTime) / 1000) / 60) + " Minutes");
		

	}
	
}
