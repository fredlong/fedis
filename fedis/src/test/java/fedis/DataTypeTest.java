package fedis;

import com.fedis.RedisProxyFactory;
import com.fedis.impl.RedisProxy;
import com.fedis.util.FedisException;
import com.fedis.util.FedisUtils;

public class DataTypeTest {

	
	public static void main(String[] args) throws FedisException{
		TestUtils.initRedis();
		
		RedisProxy proxy = RedisProxyFactory.getInstance().getRedisProxy("test");
		String keyStr = "strKey";
		String keyInteger = "integerKey";
		String keyIntegerList = "integerListKey";
		
//		proxy.set(keyStr, "hello");
//		proxy.set(keyInteger, 88);
//		proxy.lpushInteger(keyIntegerList, 888);
//		
//		System.out.println("Get string from redis , value is :"+proxy.getStr(keyStr));
//		System.out.println("Get string from redis , value is :"+proxy.getInteger(keyInteger));
	}
	
	
	
}
