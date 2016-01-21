package com.fedis.util;


public class FedisException extends Exception {
	private static final long serialVersionUID = -3553863869931397360L;

	int returnCode;

	public FedisException(String message) {
		super(message);
		this.returnCode = 500;
	}

	public FedisException(String message, Exception ex) {
		this(message, ex, 500);
		if(ex instanceof FedisException){
			this.returnCode = ((FedisException) ex).getReturnCode();
		}
	}
	
	public FedisException(String message, int returnCode) {
		this(message, null, returnCode);
	}

	public FedisException(String message, Exception ex, int returnCode) {
		super(message, ex);
		this.returnCode = returnCode;
	}

	public Exception getEx() {
		return this;
	}

	public String getMessage() {
		return super.getMessage();
	}

	public int getReturnCode() {
		return returnCode;
	}

	public void setReturnCode(int returnCode) {
		this.returnCode = returnCode;
	}
}
