/* -----------------------------------------------------------------------------
 * Copyright (c) 2013 SuccessFactors, all rights reserved.
 *
 * This software and documentation is the confidential and proprietary
 * information of SuccessFactors.  SuccessFactors makes no representation
 * or warranties about the suitability of the software, either expressed or
 * implied.  It is subject to change without notice.
 *
 * U.S. and international copyright laws protect this material.  No part
 * of this material may be reproduced, published, disclosed, or
 * transmitted in any form or by any means, in whole or in part, without
 * the prior written permission of SuccessFactors.
 * -----------------------------------------------------------------------------
 */

package com.dbvalidator.core;

import java.lang.reflect.Constructor;
import java.util.TimeZone;

import com.dbvalidator.exception.ValidationToolException;
import com.dbvalidator.validators.BaseValidator;
import com.dbvalidator.validators.Validator;

/**
 *
 * @author AJ
 */
public class HanaMigrationValidatorFactory {

	public static Validator getValidator(final String validator, final String columnName, final String tableName, final TimeZone dbTimezone)
			throws ValidationToolException {
		if (validator != null) {
			try {
				final Class<Validator> clazz = (Class<Validator>) Class.forName(validator);
				final Constructor<Validator> constructor =  clazz.getDeclaredConstructor();
				final Validator migrationvalidator = constructor.newInstance();;

				if (migrationvalidator instanceof BaseValidator) {
						((BaseValidator) migrationvalidator).setColumnName(columnName);
				    	((BaseValidator) migrationvalidator).setTableName(tableName);
				}

				return migrationvalidator;
			} catch (final Exception e) {
				throw new ValidationToolException(e);
			}
		}
		return null;
	}

	
	public static String getDefaultValidatorName(final String datatype) {

		String validatorName = null;
		if (datatype.equals("DATE")) {
			validatorName = "com.successfactors.hana.TimestampConversionValidator";
		} else if (datatype.equals("BLOB")) {
			validatorName = "com.successfactors.hana.LobValidator";
		} else if (datatype.equals("TIMESTAMP(6)") || datatype.equals("TIMESTAMP(9)") || datatype.indexOf("TIMESTAMP") >= 0) {

			if (datatype.contains("LOCAL TIME ZONE")) {
				validatorName = "com.successfactors.hana.LocalTimestampTimezoneValidator";
			} else {
				validatorName = "com.successfactors.hana.TimestampConversionValidator";
			}
		} else {
			validatorName = "com.successfactors.hana.ColumnValidator";
		}

		return validatorName;

	}

}
