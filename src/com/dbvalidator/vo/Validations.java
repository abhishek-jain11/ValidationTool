
package com.dbvalidator.vo;

import java.util.Map;

/**
 *
 * @author AJ
 */
public class Validations {

	Map<String, Validation> validations;

	public Map<String, Validation> getValidtionTests() {
		return validations;
	}

	public void setValidtionTests(final Map<String, Validation> validtionTest) {
		this.validations = validtionTest;
	}

}
