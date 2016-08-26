
package com.dbvalidator.sql;

import java.sql.ResultSet;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.vo.HanaTuple;
import com.dbvalidator.vo.OracleTuple;
import com.dbvalidator.vo.Tuple;

/**
 *
 * @author AJ
 */
public class TupleFactory {

	public static Tuple getTuple(final ResultSet rs) throws ValidationToolException {
		Tuple tuple = null;
		if (rs instanceof OracleResultSet) {
			tuple = new OracleTuple();
		} else if (rs instanceof HanaResultSet) {
			tuple = new HanaTuple();
		} else {
			throw new ValidationToolException("ResultSet Not Supported");
		}
		tuple.initialize(rs);

		return tuple;
	}

}
