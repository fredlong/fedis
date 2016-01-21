package com.fedis;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fedis.router.RedisNode;
import com.fedis.util.FedisException;



/**
 *
 * @ClassName: RedisProxyFactory
 *
 * @Description: Redis工厂代理类
 *
 * @author Viking
 *
 * @date 2014年7月18日 下午7:52:55
 * 
 *
 */
public class RedisProxyFactory {

	private static RedisProxyFactory instance = null;
	private static Object syncLock = new Object();

	private Map<String, RedisProxy> proxyMap = null;
	private Map<Integer, RedisCluster> redisMap = null;

	public static RedisProxyFactory getInstance() {
		if (instance == null) {
			synchronized (syncLock) {
				if (instance == null) {
					instance = new RedisProxyFactory();
				}
			}
		}
		return instance;
	}

	private RedisProxyFactory() {
		super();
		redisMap = new HashMap<Integer, RedisCluster>();
		proxyMap = new HashMap<String, RedisProxy>();
	}
	
	public void addRedisNode(RedisNode node) throws SQLException, FedisException{
		RedisCluster redisCluster = new RedisCluster(node);
		RedisProxy proxy = proxyMap.get(node.getRoleName());

		if (proxy == null) {
			proxy = new RedisProxy(node.getRoleName());
			proxyMap.put(node.getRoleName(), proxy);
		}
		proxy.initNode(redisCluster);
		redisMap.put(node.getId(), redisCluster);
	}

	public RedisProxy getRedisProxy(String redisGroupName) {
		RedisProxy result = proxyMap.get(redisGroupName);

		if (result == null) {
			throw new RuntimeException("no matched RedisGroup found,check table FAEDB.FAE_RedisCluster");
		}

		return result;
	}
	
	public RedisCluster getRedisNode(int id){
		return redisMap.get(id);
	}

}
