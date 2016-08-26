
package com.dbvalidator.sql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


import org.apache.logging.log4j.Logger;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.logging.LoggerFactory;
import com.dbvalidator.vo.ResultsetType;
import com.dbvalidator.vo.SQLQuery;
import com.dbvalidator.vo.Tuple;


/**
 *
 * @author AJ
 */
public class QueryExecutor {

	/*public static LinkedList<Tuple> executeQueryTable(final Connection conn, final Table table) throws HanaMigrationValidatorException {
		final String sql = getQuery(table, conn);
		return executeQueryString(conn, sql);
	}*/
	static Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

	public static Map<ResultsetType, LinkedList<Tuple>> executeSQL(final Connection oracleConnection, final Connection hanaConnection, final SQLQuery sql)
			throws ValidationToolException {
		final Map<ResultsetType, LinkedList<Tuple>> results = new HashMap<ResultsetType, LinkedList<Tuple>>();

		try {

			final LinkedList<Tuple> oracleRecords = executeQueryString(oracleConnection, sql.getOracleQuery());
			final LinkedList<Tuple> hanaRecords = executeQueryString(hanaConnection, sql.getHanaQuery());
			results.put(ResultsetType.ORACLE, oracleRecords);
			results.put(ResultsetType.HANA, hanaRecords);
		} catch (final ValidationToolException e) {
			logger.error("Unable to execute " + sql);
		}
		return results;
	}

	public static LinkedList<Tuple> executeQueryFile(final Connection conn, final String fileName) throws ValidationToolException {
		final InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
		final BufferedReader br = new BufferedReader(new InputStreamReader(in));
		final StringBuilder file = new StringBuilder();
		String sqlScript = new String();
		try {
			String temp = new String();
			while ((temp = br.readLine()) != null) {
				file.append(temp);
			}
			sqlScript = file.toString();
			br.close();
			return executeQueryString(conn, sqlScript);
		} catch (final IOException e) {
			throw new ValidationToolException(e);
		}
	}

	public static LinkedList<Tuple> executeQueryString(final Connection conn, final String sql) throws ValidationToolException {

		PreparedStatement ps = null;
		ResultSet rs = null;
		LinkedList<Tuple> records = null;
		try {
			if (sql.length() > 0) {
				logger.debug("Executing SQL " + sql);
				ps = conn.prepareStatement(sql);

				rs = ps.executeQuery();
				records = processResultset(rs);
			}
		} catch (final SQLException e) {
			logger.error("Failed executing " + sql);
			throw new ValidationToolException(e);
		} finally {
			closeFinally(ps, rs);
		}
		return records;
	}

	public static LinkedList<Tuple> processResultset(final ResultSet rs) throws ValidationToolException {
		final LinkedList<Tuple> tupleList = new LinkedList<Tuple>();
		try {
			logger.debug("Processing records from ResultSet");
			while (rs.next()) {
				final Tuple tuple = TupleFactory.getTuple(rs);
				tupleList.add(tuple);
			}
			logger.debug("Processing records Completed");
		} catch (final SQLException e) {
			throw new ValidationToolException(e);
		}
		return tupleList;
	}

	public static void closeFinally(final PreparedStatement ps, final ResultSet rs) throws ValidationToolException {
		try {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		} catch (final SQLException e) {
			throw new ValidationToolException(e);
		}
	}

}
