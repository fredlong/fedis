package com.fedis.router;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.fedis.util.FedisException;



/**
 *
 * @ClassName: GeneralHashHelper
 *
 * @Description: 直接使用JAVA的.hashCode()
 *
 * @author Viking
 *
 * @date 2014年7月18日 下午7:58:50
 * 
 *
 */
public class GeneralHashHelper implements HashHelper {

	private static MessageDigest messageDigest = null;

	public GeneralHashHelper() {
		super();
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ignore) {
		}
	}

	@Override
	public int hash(String keyValue) throws FedisException {
		synchronized (messageDigest) {
			try {
				byte[] utf8Bytes = keyValue.getBytes("UTF-8");
				messageDigest.update(utf8Bytes);
				byte[] resultByteArray = messageDigest.digest();
				String md5Result = byteToStr(resultByteArray);
				int hashResult = md5Result.hashCode();
				return hashResult;
			} catch (Exception e) {
				throw new FedisException("hash error", e);
			}
		}
	}

	private String byteToStr(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bytes) {
			sb.append(b);
		}
		return sb.toString();
	}

	// public static void main(String args[]) throws Exception {
	// System.out.println("start");
	// //
	// // 初始化FAE
	// ServiceSettings.init("settings.properties");
	// AppEngineManager.INSTANCE.initialize();
	// AppEngineManager.INSTANCE.registerLocatorType("GlobalLocator",
	// GlobalLocator.class);
	//
	// GeneralHashHelper helper = new GeneralHashHelper();
	//
	// int result = helper.hash("testValueewlsdajkfksalfjal;kjdf;lkf");
	// long start = System.currentTimeMillis();
	// for (Integer i = 5000000; i < 10000000; i++) {
	// if(result != helper.hash("testValueewlsdajkfksalfjal;kjdf;lkf")){
	// throw new RuntimeException("WTF");
	// }
	// }
	//
	// long end = System.currentTimeMillis();
	// System.out.println("finished" + (end - start));

	// String test = "123123123123";
	//
	// System.out.println(toString(test));
	//
	// for(int i=0;i<100;i++){
	// System.out.println(tesHhash(test));
	// }

	// }
	//
	// private static String toString(Object o){
	// return o.toString();
	// }
	//
	// private static int tesHhash(Object o) {
	// MessageDigest messageDigest;
	// try {
	// messageDigest = MessageDigest.getInstance("MD5");
	// messageDigest.update(o.toString().getBytes());
	// byte[] resultByteArray = messageDigest.digest();
	// return (new String(resultByteArray)).hashCode();
	// } catch (NoSuchAlgorithmException e) {
	// return 0;
	// }
	//
	// }

}