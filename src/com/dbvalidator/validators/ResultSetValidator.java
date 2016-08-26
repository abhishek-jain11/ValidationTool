
package com.dbvalidator.validators;

import java.util.LinkedList;

import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.logging.LoggerFactory;
import com.dbvalidator.vo.Tuple;
import com.dbvalidator.vo.ValidationResultVO;

/**
 *
 * @author AJ
 */
public class ResultSetValidator extends BaseValidator {
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public int validate(final LinkedList<Tuple> oracleRecords, final LinkedList<Tuple> hanaRecords) throws ValidationToolException {

		int i = 0;
		int noOfErrors = 0;
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
		ValidationResultVO result = null;
		if (oracleRecord != null) {
			if (!oracleRecord.equals(hanaRecord)) {

				final StringBuilder error = new StringBuilder();
				result = new ValidationResultVO();
				error.append(oracleRecord.toString());
				error.append(hanaRecord.toString());
				result.setError(error.toString());
				result.setValidator(this.getClass().getName());
				logger.error(error.toString());
				//noOfErrors++;
			}
		} else if (hanaRecord != null) {
			if (!hanaRecord.equals(oracleRecord)) {
				result = new ValidationResultVO();
				result.setError("Oracle record " + oracleRecord + " Hana Record " + hanaRecord);
				result.setValidator(this.getClass().getName());
				//results.add(result)
				logger.error("Oracle record " + oracleRecord + " Hana Record " + hanaRecord);
			}

		}
		return result;
	}

}
