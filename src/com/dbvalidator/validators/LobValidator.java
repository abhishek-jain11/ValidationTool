
package com.dbvalidator.validators;

import java.util.LinkedList;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.helper.ObjectUtils;
import com.dbvalidator.logging.LoggerFactory;
import com.dbvalidator.vo.Tuple;
import com.dbvalidator.vo.ValidationResultVO;



public class LobValidator extends BaseValidator {

	
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public int validate(final LinkedList<Tuple> oracleRecords, final LinkedList<Tuple> hanaRecords) throws ValidationToolException {
		int noOfErrors = 0;
		int i = 0;
		for (final Tuple oracleRecord : oracleRecords) {
			final Tuple hanaRecord = hanaRecords.get(i);

			final ValidationResultVO result = validate(oracleRecord, hanaRecord);
			if (result != null) {
				noOfErrors++;
			}
			i++;
		}
		return noOfErrors;

	}

	@Override
	public ValidationResultVO validate(final Tuple oracleRecord, final Tuple hanaRecord) throws ValidationToolException {
		String oracleDigest = null;
		String hanaDigest = null;
		ValidationResultVO result = null;
		final Map<String, Object> oracleEntries = oracleRecord.getTuple();
		if (oracleEntries != null) {

			oracleDigest = (String) oracleEntries.get(getColumnName());
		}

		final Map<String, Object> hanaEntries = hanaRecord.getTuple();

		if (oracleEntries != null) {

			hanaDigest = (String) hanaEntries.get(getColumnName());
		}

		logger.debug("Oracle Digest " + oracleDigest + " Hana Digest " + hanaDigest);
		if (!ObjectUtils.equals(hanaDigest, oracleDigest)) {
			String error = "Failed validating Column:" + getColumnName() + " Table: " + getTableName() + "/n Oracle Digest " + oracleDigest + " Hana Digest "
					+ hanaDigest + "\n";
			result = new ValidationResultVO();
			error += "/n Record Details:/n";
			error += oracleRecord.toString() + "/n";
			error += hanaRecord.toString() + "/n";
			logger.error(error);
			result.setError(error);
			result.setValidator(this.getClass().getName());
		}
		return result;

	}

}
