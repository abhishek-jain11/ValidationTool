package com.dbvalidator.exception;

public class ValidationToolException extends Exception {

	private static final long serialVersionUID = 1L;


	public ValidationToolException(final Exception e) {
		super(e);
	}

	public ValidationToolException(final String e) {
		super(e);
	}
}
