package fedis;

import java.util.HashMap;

import redis.clients.util.SafeEncoder;

import com.fedis.RedisProxyFactory;
import com.fedis.impl.RedisProxy;
import com.fedis.router.RedisNode;

public class Test {
	public static void main(String[] args) throws Exception{

		TestUtils.initRedis();

		
		
		RedisProxy proxy = RedisProxyFactory.getInstance().getRedisProxy("test");
		
		System.out.println(proxy.getString("11"));
		
		
		
		String key1 = "1";
		String key2 = "2";
		String key3 = "3";
		String key4 = "4";
		String key5 = "5";
		String key6 = "6";
		String key7 = "7";
		String key8 = "8";
		String key9 = "9";
		String key10 = "10";
		
		
		proxy.set(key1, 1);
		proxy.set(key2, 2);
		proxy.set(key3, 3);
		proxy.set(key4, 4);
		proxy.set(key5, 5);
		proxy.set(key6, 6);
		proxy.set(key7, 7);
		proxy.set(key8, 8);
		proxy.set(key9, 9);
		proxy.set(key10, 10);
		
		System.out.println(proxy.getString(key1));
		System.out.println(proxy.getString(key2));
		System.out.println(proxy.getString(key3));
		System.out.println(proxy.getString(key4));
		System.out.println(proxy.getString(key5));
		System.out.println(proxy.getString(key6));
		System.out.println(proxy.getString(key7));
		System.out.println(proxy.getString(key8));
		System.out.println(proxy.getString(key9));
		System.out.println(proxy.getString(key10));
		
		

//		
//		String key = "hello";
//		HashMap<String , String> hm = new HashMap<String , String>();
//		hm.put("abc", "abcd");
//		proxy.hmset(key, hm);
		//get 
	}
}
