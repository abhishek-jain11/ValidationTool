package com.dbvalidator.validators;

import java.util.LinkedList;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.vo.Tuple;
import com.dbvalidator.vo.ValidationResultVO;

public class BaseValidator implements Validator{

	String columnName;
	String tableName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(final String columnName) {
		this.columnName = columnName;
	}

	@Override
	public int validate(LinkedList<Tuple> sourceRecords, LinkedList<Tuple> targetRecords) throws ValidationToolException {
	
		return 0;
	}

	@Override
	public ValidationResultVO validate(Tuple oracleRecord, Tuple hanaRecord) throws ValidationToolException {
		
		return null;
	}

}
