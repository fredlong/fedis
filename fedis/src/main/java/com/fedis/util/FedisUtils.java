package com.fedis.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;

import com.fedis.protobuff.ProtoEntity;

public class FedisUtils {
	public static final String EMPTY_STRING = "";
	public static boolean isNullOrEmpty(String str)
	{
		return str == null ? true : str.equals(EMPTY_STRING);
	}
	
	public static String safeParseString(byte[] bytes) throws FedisException{
		if(null == bytes){
			return null;
		}
		else{
			try {
			    return new String(bytes, Protocol.CHARSET);
			} catch (UnsupportedEncodingException e) {
			    throw new FedisException("transfer bytes to String meets error" , e);
			}
		}
	}
	
	
	public static Long safeParseLong(byte[] bytes) throws FedisException{
		if(null == bytes){
			return null;
		}
		else{
			try {
			    String str = new String(bytes, Protocol.CHARSET);
			    return Long.parseLong(str);
			} catch (Exception e) {
			    throw new FedisException("transfer bytes to Integer meets error" , e);
			}
		}
	}
	
	public static Integer safeParseInteger(byte[] bytes) throws FedisException{
		if(null == bytes){
			return null;
		}
		else{
			try {
			    String str = new String(bytes, Protocol.CHARSET);
			    return Integer.parseInt(str);
			} catch (Exception e) {
			    throw new FedisException("transfer bytes to Integer meets error" , e);
			}
		}
	}
	
	public static Integer safeParseInteger(String str) throws FedisException{
		if(null == str){
			return null;
		}
		else{
			try {
			    return Integer.parseInt(str);
			} catch (Exception e) {
			    throw new FedisException("transfer bytes to Integer meets error" , e);
			}
		}
	}
	
	public static <T extends ProtoEntity> T safeParseProto(byte[] bytes , final Class<T> protoClass) throws FedisException{
		if(bytes != null){
			T value = getInstance(protoClass);
			value.parseFrom(bytes);
			return value;
		}
		else
		{
			return null;
		}
		
	}
	
	public static <T extends ProtoEntity> List<T> safeParseBytesToProtos(List<byte[]> bytes, final Class<T> protoClass) throws FedisException{
		if(bytes != null){
			List<T> result = new ArrayList<T>();
			for(int i = 0 ; i < bytes.size() ; i++){
				T value = getInstance(protoClass);
				value.parseFrom(bytes.get(i));
				result.add(value);
			}
			
			return result;
		}
		else
		{
			return null;
		}
		
	}
	
	public static List<String> safeParseBytesToStrings(List<byte[]> bytes) throws FedisException{
		if(bytes != null){
			List<String> result = new ArrayList<String>();
			for(int i = 0 ; i < bytes.size() ; i++){
				result.add(SafeEncoder.encode(bytes.get(i)));
			}
			
			return result;
		}
		else
		{
			return null;
		}
		
	}
	
	public static List<Integer> safeParseBytesToIntegers(List<byte[]> bytes) throws FedisException{
		if(bytes != null){
			List<Integer> result = new ArrayList<Integer>();
			for(int i = 0 ; i < bytes.size() ; i++){
				String str = SafeEncoder.encode(bytes.get(i));
				result.add(Integer.parseInt(str));
			}
			
			return result;
		}
		else
		{
			return null;
		}
		
	}
	
	public static Set<String> safeParseBytesToStrings(Set<byte[]> byteSet) throws FedisException{
		if(byteSet != null){
			Set<String> result = new HashSet<String>();
			for (byte[] bytes : byteSet) {
				result.add(SafeEncoder.encode(bytes));
			}
			
			return result;
		}
		else
		{
			return null;
		}
		
	}
	
	
	public static <T extends ProtoEntity> Map<String, T> safeParseBytesToProtos(Map<byte[], byte[]> bytes, final Class<T> protoClass) throws FedisException{
		if(bytes != null){
			Map<String, T> result = new HashMap<String, T>();
			for (Entry<byte[], byte[]> bufferEntry : bytes.entrySet()) {
				result.put(SafeEncoder.encode(bufferEntry.getKey()), FedisUtils.safeParseProto(bufferEntry.getValue(), protoClass));
			}
			
			return result;
		}
		else
		{
			return null;
		}
		
	}
	
	public static Map<String, String> safeParseBytesToStrings(Map<byte[], byte[]> bytes) throws FedisException{
		if(bytes != null){
			Map<String, String> result = new HashMap<String, String>();
			for (Entry<byte[], byte[]> bufferEntry : bytes.entrySet()) {
				result.put(SafeEncoder.encode(bufferEntry.getKey()), SafeEncoder.encode(bufferEntry.getValue()));
			}
			
			return result;
		}
		else
		{
			return null;
		}
		
	}
	
	public static Map<String, Integer> safeParseBytesToIntegers(Map<byte[], byte[]> bytes) throws FedisException{
		if(bytes != null){
			Map<String, Integer> result = new HashMap<String, Integer>();
			for (Entry<byte[], byte[]> bufferEntry : bytes.entrySet()) {
				result.put(SafeEncoder.encode(bufferEntry.getKey()), Integer.parseInt(SafeEncoder.encode(bufferEntry.getValue())));
			}
			
			return result;
		}
		else
		{
			return null;
		}
		
	}
	

	public static <T extends ProtoEntity> List<byte[]> safeParseProtosToBytes(T[] values) throws FedisException{
		checkNull(values);

		List<byte[]> result = new ArrayList<byte[]>();
		for(int i = 0 ; i < values.length ; i++){
			checkNull(values[i]);
			result.add(values[i].toByteArray());
		}
		
		return result;

		
	}
	
	public static List<byte[]> safeParseStringsToBytes(String[] values) throws FedisException{
		checkNull(values);

		List<byte[]> result = new ArrayList<byte[]>();
		for(int i = 0 ; i < values.length ; i++){
			checkNull(values[i]);
			result.add(SafeEncoder.encode(values[i]));
		}
		
		return result;

		
	}
	
	public static List<byte[]> safeParseIntegersToBytes(Integer[] values) throws FedisException{
		checkNull(values);
		
		List<byte[]> result = new ArrayList<byte[]>();
		for(int i = 0 ; i < values.length ; i++){
			checkNull(values[i]);
			result.add(SafeEncoder.encode(String.valueOf(values[i])));
		}
		
		return result;

		
	}
	
	public static <T extends ProtoEntity> Map<byte[] , byte[]> safeParseProtosToBytes(Map<String , T> values) throws FedisException{
		checkNull(values);

		Map<byte[] , byte[]> result = new HashMap<byte[] , byte[]>();
		for (Entry<String, T> entry : values.entrySet()) {
			checkNull(entry.getValue());
			byte[] jedisKey = SafeEncoder.encode(entry.getKey());
			byte[] jedisValue = entry.getValue().toByteArray();

			result.put(jedisKey, jedisValue);
		}
		
		return result;

		
	}
	
	public static Map<byte[] , byte[]> safeParseStringsToBytes(Map<String , String> values) throws FedisException{
		checkNull(values);

		Map<byte[] , byte[]> result = new HashMap<byte[] , byte[]>();
		for (Entry<String, String> entry : values.entrySet()) {
			checkNull(entry.getValue());
			byte[] jedisKey = SafeEncoder.encode(entry.getKey());
			byte[] jedisValue = SafeEncoder.encode(entry.getValue());

			result.put(jedisKey, jedisValue);
		}
		
		return result;

		
	}
	
	public static Map<byte[] , byte[]> safeParseIntegersToBytes(Map<String , Integer> values) throws FedisException{
		checkNull(values);

		Map<byte[] , byte[]> result = new HashMap<byte[] , byte[]>();
		for (Entry<String, Integer> entry : values.entrySet()) {
			checkNull(entry.getValue());
			byte[] jedisKey = SafeEncoder.encode(entry.getKey());
			byte[] jedisValue = SafeEncoder.encode(String.valueOf(entry.getValue()));

			result.put(jedisKey, jedisValue);
		}
		
		return result;

		
	}
	

	public static <T> T getInstance(Class<T> clazz) throws FedisException {
		try {
			return clazz.newInstance();
		} catch (InstantiationException e) {
			throw new FedisException(String.format("call %s.newInstance() failed:%s", clazz.getName(), e.getMessage()), e);
		} catch (IllegalAccessException e) {
			throw new FedisException(String.format("call %s.newInstance() failed:%s", clazz.getName(), e.getMessage()), e);
		}
	}

	public static void checkNull(Object value) throws FedisException {
		if(null == value)
		{
			throw new FedisException("value sent to redis cannot be null" , 400);
		}
	}
	
}
