package com.fedis.util;

public class FedisUtils {
	public static final String EMPTY_STRING = "";
	public static boolean isNullOrEmpty(String str)
	{
		return str == null ? true : str.equals(EMPTY_STRING);
	}
}
