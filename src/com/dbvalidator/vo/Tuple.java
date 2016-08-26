
package com.dbvalidator.vo;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.Logger;
import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.helper.ObjectUtils;
import com.dbvalidator.logging.LoggerFactory;


public class Tuple {
	Map<String, Object> tuple;
	Logger logger = LoggerFactory.getLogger(Tuple.class);

	public Map<String, Object> getTuple() {
		return tuple;
	}

	public void setTuple(final Map<String, Object> tuple) {
		this.tuple = tuple;
	}

	public void initialize(final ResultSet rs) throws ValidationToolException {
		try {
			final ResultSetMetaData rsMetadata = rs.getMetaData();
			final int totalColumns = rsMetadata.getColumnCount();
			tuple = new HashMap<String, Object>(totalColumns);
			for (int i = 1; i <= totalColumns; i++) {
				final String columnName = rsMetadata.getColumnLabel(i);
				final String columnTypeName = rsMetadata.getColumnTypeName(i);

				final int objectType = rsMetadata.getColumnType(i);

				Object data = null;
				if (objectType == Types.BLOB) {
					final InputStream is = rs.getBinaryStream(i);
					data = getMD5(is);
				} else if (objectType == Types.NCLOB) {
					data = rs.getString(i);
				} else if (objectType == Types.CLOB) {

					data = rs.getString(i);
				} else if (objectType == Types.TIMESTAMP) {
					final Timestamp t = rs.getTimestamp(i);
					//Removing microsecond component from timestamp
					if (t != null) {
						final int nanos = (t.getNanos() / 1000000) * 1000000;
						t.setNanos(nanos);
					}
					data = t;

				} else if (objectType == Types.NUMERIC) {
					final String doub = rs.getString(i);
					if (doub != null && !doub.isEmpty()) {
						data = new BigDecimal(doub);
					}
				} else if (objectType == Types.FLOAT) {
					final String doub = rs.getString(i);
					if (doub != null && !doub.isEmpty()) {
						data = new BigDecimal(doub);
					}
				} else if (objectType == Types.DECIMAL) {
					final String doub = rs.getString(i);
					if (doub != null && !doub.isEmpty()) {
						data = new BigDecimal(doub);
					}

				} else {
					if ("TIMESTAMP WITH LOCAL TIME ZONE".equals(columnTypeName)) {
						final Timestamp t = rs.getTimestamp(i);
						//Removing microsecond component from timestamp
						if (t != null) {
							final int nanos = (t.getNanos() / 1000000) * 1000000;
							t.setNanos(nanos);
						}
						data = t;

					} else {
						data = rs.getObject(i);
					}
				}

				tuple.put(columnName, data);
			}

		} catch (final SQLException e) {
			logger.error("Failed creating tuple out of resultset");
			throw new ValidationToolException(e);
		}
	}

	public String getMD5(final InputStream in) throws ValidationToolException {
		try {
			final MessageDigest md = MessageDigest.getInstance("MD5");
			md.reset();
			byte[] digest = null;
			String encodedHex = null;
			if (in != null) {
				final DigestInputStream dis = new DigestInputStream(in, md);

				while (dis.read() != -1) {

				}

				digest = md.digest();
				in.close();
				encodedHex = digest.toString();  
						
			}
			
			return encodedHex;
		} catch (final NoSuchAlgorithmException e) {
			throw new ValidationToolException(e);
		} catch (final IOException e) {
			throw new ValidationToolException(e);
		}

	}

	@Override
	public String toString() {
		final StringBuilder error = new StringBuilder();
		//String error = "";
		String recordDelimiter = "";
		final String recordDelimiter2 = " || ";
		final String space = " ";
		final String colon = " : ";
		for (final Entry<String, Object> entry : getTuple().entrySet()) {
			error.append(recordDelimiter);
			error.append(space);
			error.append(entry.getKey());
			error.append(colon);
			error.append(entry.getValue());
			recordDelimiter = recordDelimiter2;
		}
		return error.toString();
	}

	public boolean equals(final Tuple otherTuple) {
		if (otherTuple == null) {
			return false;
		}

		for (final Entry<String, Object> entry : tuple.entrySet()) {
			final String tupleKey = entry.getKey();
			final Object tupleValue = entry.getValue();
			final Object otherTupleValue = otherTuple.getTuple().get(tupleKey);

			if (!ObjectUtils.equals(tupleValue, otherTupleValue)) {
				logger.debug("Comparing " + tupleKey + " " + tupleValue + " " + otherTupleValue);
				return false;
			}
		}

		return true;

	}

}
