
package com.dbvalidator.validators;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedList;

import java.util.Map;
import java.util.TimeZone;

import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.helper.ObjectUtils;
import com.dbvalidator.logging.LoggerFactory;
import com.dbvalidator.vo.Tuple;
import com.dbvalidator.vo.ValidationResultVO;


/**
 * @author AJ
 */
public class LocalTimestampTimezoneValidator extends BaseValidator {

	final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public int validate(final LinkedList<Tuple> oracleRecords, final LinkedList<Tuple> hanaRecords) throws ValidationToolException {
		int i = 0;
		int noOfErrorsFound = 0;
		for (final Tuple oracleRecord : oracleRecords) {
			//	hanaRecords

			final Tuple hanaRecord = hanaRecords.get(i);

			final ValidationResultVO result = validate(oracleRecord, hanaRecord);
			if (result != null) {
				noOfErrorsFound++;
			}
			i++;
		}

		return noOfErrorsFound;
	}

	@Override
	public ValidationResultVO validate(final Tuple oracleRecord, final Tuple hanaRecord) throws ValidationToolException {

		Timestamp oracleTimestamp = null;
		Timestamp hanaTimestamp = null;
		Timestamp convertedTimestamp = null;
		ValidationResultVO result = null;

		final Map<String, Object> oracleEntries = oracleRecord.getTuple();
		if (oracleEntries != null) {
			oracleTimestamp = (Timestamp) oracleEntries.get(getColumnName());
		}
		final TimeZone timezone = Calendar.getInstance().getTimeZone();

		if (oracleTimestamp != null) {
			convertedTimestamp = ObjectUtils.convertTimestamp(oracleTimestamp, timezone);
		}
		final Map<String, Object> hanaEntries = hanaRecord.getTuple();
		if (hanaEntries != null) {
			hanaTimestamp = (Timestamp) hanaEntries.get(getColumnName());
		}
		logger.debug("Validating Oracle TimestampWithTimezone  " + oracleTimestamp + " ConvertedUTCTimestamp " + convertedTimestamp + " HanaTimestamp "
				+ hanaTimestamp);
		if (!ObjectUtils.equals(hanaTimestamp, convertedTimestamp)) {
			final StringBuilder error = new StringBuilder();
			error.append("Failed Validating ");
			error.append("Column: " + getColumnName() + " Table: " + getTableName() + "\n");
			error.append("Oracle Timestamp ");
			error.append(oracleTimestamp);
			error.append(" ConvertedUTCTimestamp ");
			error.append(convertedTimestamp);
			error.append(" HanaTimestamp ");
			error.append(hanaTimestamp);
			error.append("\n Record Details:\n");
			error.append(oracleRecord.toString());
			error.append("\n");
			error.append(hanaRecord.toString());
			error.append("\n");
			result = new ValidationResultVO();
			logger.error(error.toString());
			result.setError(error.toString());
			//result.setValidator(this.getClass().getName());
		}
		return result;
	}
}
