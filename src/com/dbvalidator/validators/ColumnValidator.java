
package com.dbvalidator.validators;

import java.util.LinkedList;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.helper.ObjectUtils;
import com.dbvalidator.logging.LoggerFactory;
import com.dbvalidator.vo.Tuple;
import com.dbvalidator.vo.ValidationResultVO;

/**
 *
 * @author AJ
 */
public class ColumnValidator extends BaseValidator {

	final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public int validate(final LinkedList<Tuple> oracleRecords, final LinkedList<Tuple> hanaRecords) throws ValidationToolException {

		int i = 0;
		int noOfErrors = 0;
		for (final Tuple oracleRecord : oracleRecords) {
			logger.debug("At record " + i);
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
		ValidationResultVO result = null;
		final Map<String, Object> oracleValues = oracleRecord.getTuple();
		Object oracleData = null;
		Object hanaData = null;

		if (oracleValues != null) {
			oracleData = oracleValues.get(getColumnName());
			logger.debug("Oracle data " + oracleData);
		}

		final Map<String, Object> hanaValues = hanaRecord.getTuple();
		if (hanaValues != null) {
			hanaData = hanaValues.get(getColumnName());
			logger.debug("Hana data " + hanaData);
		}

		
			if (!ObjectUtils.equals(oracleData, hanaData)) {
				result = new ValidationResultVO();
				String error = "Failed validating Column: " + columnName + " Table: " + tableName;
				error += oracleRecord.toString() + "\n";
				error += hanaRecord.toString();
				result.setError(error);
				logger.error(error);
				result.setValidator(this.getClass().getName());
			}

		
		return result;
	}
}
