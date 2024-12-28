package com.fedis;


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Client;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

import com.fedis.router.ReRoutable;
import com.fedis.router.RedisNode;
import com.fedis.util.FedisException;
import com.fedis.util.FedisUtils;


public class RedisCluster implements ReRoutable{

	private RedisNode node;

	private JedisPool jedisPool;

	public RedisCluster(RedisNode node){
		this.node = node;
		this.jedisPool = new JedisPool(node.getHost(), node.getPort());
	}

	public RedisNode getRedisNode(){
		return node;
	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public void setJedisPool(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	@Override
	public String toString() {
		return String.format("id=%s,groupName=%s,host=%s,port=%s,weight=%s", node.getId(), node.getRoleName(), node.getHost(), node.getPort(), node.getWeight());
	}

	/**
	 * 用于执行直接的Redis命令，仅用于运维平台，生产环境开发请勿调用
	 */
	public Object runRawCommand(String rawCommand) throws FedisException {
		Jedis jedis = null;
		boolean isBroken = false;
		try {
			if (FedisUtils.isNullOrEmpty(rawCommand)) {
				return "bad command";
			}
			rawCommand = rawCommand.trim();
			String[] splitResult = rawCommand.split(" ");
			if (splitResult.length < 2) {
				return "bad command";
			}
			String commandStr = splitResult[0];
			List<byte[]> parasList = new ArrayList<byte[]>();
			if (splitResult.length > 1) {
				for (int i = 1; i < splitResult.length; i++) {
					parasList.add(SafeEncoder.encode(splitResult[i]));
				}
			}

			Command command = null;
			try {
				command = Command.valueOf(commandStr.toUpperCase(Locale.ENGLISH));
			} catch (Exception e) {
				return "No Such Command";
			}

			byte[][] paras = parasList.toArray(new byte[0][0]);

			// 反射获取当前jedis实例的client域
			jedis = this.jedisPool.getResource();
			Field clientField = BinaryJedis.class.getDeclaredField("client");
			clientField.setAccessible(true);
			Client client = (Client) clientField.get(jedis);

			// 反射获取命令对应的方法
			Method sendCommandMethod = Connection.class.getDeclaredMethod("sendCommand", Command.class, byte[][].class);
			sendCommandMethod.setAccessible(true);
			sendCommandMethod.invoke(client, command, paras);
			return client.getOne();
		} catch (JedisDataException e) {
			return e.getMessage();
		} catch (Exception e) {
			isBroken = true;
			throw new FedisException("runRawCommand Error", e);
		} finally {
			if (jedis != null) {
				if (isBroken) {
					this.jedisPool.returnBrokenResource(jedis);
				} else {
					this.jedisPool.returnResource(jedis);
				}
			}
		}
	}

	@Override
	public Set<String> getAllDataKey(String[] keyPatterns) throws FedisException {
		Jedis jedis = null;
		boolean isBroken = false;
		try {
			jedis = this.jedisPool.getResource();
			if (keyPatterns != null && keyPatterns.length > 0) {
				Set<String> result = new HashSet<String>();
				for (String pattern : keyPatterns) {
					result.addAll(jedis.keys(pattern));
				}
				return result;
			} else {
				return jedis.keys("*");
			}
		} catch (Exception e) {
			isBroken = true;
			throw new FedisException("getAllDataKey Error", e);
		} finally {
			if (jedis != null) {
				if (isBroken) {
					this.jedisPool.returnBrokenResource(jedis);
				} else {
					this.jedisPool.returnResource(jedis);
				}
			}
		}
	}

	@Override
	public void deleteData(String dataKey) throws FedisException {
		Jedis jedis = null;
		boolean isBroken = false;
		try {
			jedis = this.jedisPool.getResource();
			if (dataKey != null) {
				jedis.del(dataKey);
			}
		} catch (Exception e) {
			isBroken = true;
			throw new FedisException("deleteDataKey Error", e);
		} finally {
			if (jedis != null) {
				if (isBroken) {
					this.jedisPool.returnBrokenResource(jedis);
				} else {
					this.jedisPool.returnResource(jedis);
				}
			}
		}
	}

	@Override
	public void moveData(String dataKey, ReRoutable destNode) throws FedisException {
		Jedis srcJedis = null;
		Jedis destJedis = null;
		RedisCluster destRedis = null;
		boolean isBroken = false;
		try {
			if (destNode instanceof RedisCluster) {
				System.out.println(String.format("begin migrate data, dataKey=%s, srcRedis=%s, destRedis=%s", dataKey, this.toString(), destNode.toString()));

				boolean renamed = false;
				destRedis = (RedisCluster) destNode;
				destJedis = destRedis.getJedisPool().getResource();

				// 如果目标节点已含有该数据key，则先删除目标节点的数据，覆盖性迁移
				if (destJedis.exists(dataKey)) {
					destJedis.rename(dataKey, dataKey + "_bak");
					renamed = true;
				}

				srcJedis = this.jedisPool.getResource();
				String result = srcJedis.migrate(destRedis.getRedisNode().getHost(), destRedis.getRedisNode().getPort(), dataKey, 0, 3000);

				if (!"OK".equals(result)) {
					if (renamed) {
						destJedis.rename(dataKey + "_bak", dataKey);
					}
					throw new FedisException(String.format("begin migrate data, dataKey=%s, srcRedis=%s, destRedis=%s, message=%s", dataKey, this.toString(), destNode.toString(),
							result));
				} else if (renamed) {
					destJedis.del(dataKey + "_bak");
				}
			} else {
				throw new FedisException(String.format("destNode is not a RedisCluster, infact = %s", destNode));
			}
		} catch (Exception e) {
			isBroken = true;
			throw new FedisException(String.format("move data failed, dataKey=%s, srcRedis=%s, destRedis=%s", dataKey, this.toString(), destNode.toString()), e);
		} finally {
			if (srcJedis != null) {
				if (isBroken) {
					this.jedisPool.returnBrokenResource(srcJedis);
				} else {
					this.jedisPool.returnResource(srcJedis);
				}
			}
			if (destJedis != null && destRedis != null) {
				destRedis.getJedisPool().returnResource(destJedis);
			}
		}
	}


}

