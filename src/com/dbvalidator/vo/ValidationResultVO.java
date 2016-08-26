
package com.dbvalidator.vo;


public class ValidationResultVO {
	String validator;
	String error;
	Tuple oracleTuple;
	Tuple hanaTuple;

	public String getError() {
		return error;
	}

	public void setError(final String error) {
		this.error = error;
	}

	public String getValidator() {
		return validator;
	}

	public void setValidator(final String validator) {
		this.validator = validator;
	}

	public Tuple getOracleTuple() {
		return oracleTuple;
	}

	public void setOracleTuple(final Tuple oracleTuple) {
		this.oracleTuple = oracleTuple;
	}

	public Tuple getHanaTuple() {
		return hanaTuple;
	}

	public void setHanaTuple(final Tuple hanaTuple) {
		this.hanaTuple = hanaTuple;
	}

}
