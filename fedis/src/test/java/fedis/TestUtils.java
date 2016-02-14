package fedis;

import com.fedis.RedisProxyFactory;
import com.fedis.router.RedisNode;
import com.fedis.util.FedisException;

public class TestUtils {
	
	static String nodeName= "test";
	
	public static void initRedis() throws FedisException{
		RedisNode node = new RedisNode();
		node.setId(0);
		node.setHost("10.211.55.5");
		node.setPort(6379);
		node.setWeight(50);
		node.setRoleName(nodeName);
		
		RedisNode node2 = new RedisNode();
		node2.setId(1);
		node2.setHost("10.211.55.5");
		node2.setPort(6380);
		node2.setWeight(50);
		node2.setRoleName(nodeName);
		
		RedisProxyFactory.getInstance().addRedisNode(node);
		RedisProxyFactory.getInstance().addRedisNode(node2);
		

	}
}
