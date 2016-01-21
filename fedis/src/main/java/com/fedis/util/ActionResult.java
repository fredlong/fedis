package com.fedis.util;

public class ActionResult<T> {
	
	private T value = null;

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

}
