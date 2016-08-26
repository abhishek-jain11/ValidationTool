package com.dbvalidator.validators;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.vo.*;

import java.util.LinkedList;

public interface Validator {

		public int validate(LinkedList<Tuple> sourceRecords, LinkedList<Tuple> targetRecords) throws ValidationToolException;

		public ValidationResultVO validate(Tuple oracleRecord, Tuple hanaRecord) throws ValidationToolException;
	
}
