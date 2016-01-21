package fedis;

import java.util.HashMap;

import com.fedis.RedisProxy;
import com.fedis.RedisProxyFactory;
import com.fedis.router.RedisNode;

public class Test {
	public static void main(String[] args) throws Exception{
		RedisNode node = new RedisNode();
		node.setId(0);
		node.setHost("10.211.55.5");
		node.setPort(6379);
		node.setWeight(50);
		node.setRoleName("Test");
		
		RedisNode node2 = new RedisNode();
		node2.setId(1);
		node2.setHost("10.211.55.5");
		node2.setPort(6380);
		node2.setWeight(50);
		node2.setRoleName("Test");
		
		RedisProxyFactory.getInstance().addRedisNode(node);
		RedisProxyFactory.getInstance().addRedisNode(node2);
		
		RedisProxy proxy = RedisProxyFactory.getInstance().getRedisProxy(node.getRoleName());
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
		
		System.out.println(proxy.getStr(key1));
		System.out.println(proxy.getStr(key2));
		System.out.println(proxy.getStr(key3));
		System.out.println(proxy.getStr(key4));
		System.out.println(proxy.getStr(key5));
		System.out.println(proxy.getStr(key6));
		System.out.println(proxy.getStr(key7));
		System.out.println(proxy.getStr(key8));
		System.out.println(proxy.getStr(key9));
		System.out.println(proxy.getStr(key10));
		
		String key = "hello";
		HashMap<String , String> hm = new HashMap<String , String>();
		hm.put("abc", "abcd");
		proxy.hmset(key, hm);
		//get 
	}
}
