package com.fedis.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import redis.clients.util.SafeEncoder;

import com.fedis.protobuff.ProtoEntity;
import com.fedis.util.Action;
import com.fedis.util.ActionResult;
import com.fedis.util.FedisException;
import com.fedis.util.FedisUtils;


class HashCommandImpl extends FedisCommandImpl{

	HashCommandImpl(RedisProxy redisProxy) {
		super(redisProxy);
	}
	
	/**
	 * 从Redis中删除指定哈希表中的一个或多个字段
	 *
	 * @param key Redis中哈希表的键
	 * @param field 哈希表中要删除的字段
	 * @return 返回删除的字段数量
	 * @throws FedisException 如果操作失败或连接出错，则抛出此异常
	 */
	Long hdel(final String key, final String field) throws FedisException {
		// 创建一个用于存储操作结果的容器
		final ActionResult<Long> result = new ActionResult<Long>();

		// 使用Redis代理执行命令
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				// 执行hdel命令，并将结果编码后存入结果容器中
				result.setValue(jedis.hdel(SafeEncoder.encode(key), SafeEncoder.encode(field)));
			}
		});

		// 返回操作结果
		return result.getValue();
	}
	
	Long hdel(final String key, final Integer field) throws FedisException {
		FedisUtils.checkNull(field);
		return hdel(key , String.valueOf(field));
	}
	
	Long hdel(final String key, final Long field) throws FedisException {
		FedisUtils.checkNull(field);
		return hdel(key , String.valueOf(field));
	}
	

	Boolean hexists(final String key, final String field) throws FedisException {
		final ActionResult<Boolean> result = new ActionResult<Boolean>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hexists(SafeEncoder.encode(key), SafeEncoder.encode(field)));
			}
		});

		return result.getValue();
	}

	<T extends ProtoEntity> T hgetProto(final String key, final String field, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseProto(hget(key ,  field) , protoClass);
	}
	
	Integer hgetInteger(final String key, final String field) throws FedisException {
		return FedisUtils.safeParseInteger(hget(key , field)); 
	}
	
	Long hgetLong(final String key, final String field) throws FedisException {
		return FedisUtils.safeParseLong(hget(key , field)); 
	}

	String hgetString(final String key, final String field) throws FedisException {
		return FedisUtils.safeParseString(hget(key , field)); 
	}
	

	byte[] hget(final String key, final String field) throws FedisException {
		final ActionResult<byte[]> result = new ActionResult<byte[]>();

		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hget(SafeEncoder.encode(key), SafeEncoder.encode(field)));
			}
		});

		return result.getValue();
	}

	
	

	<T extends ProtoEntity> Map<String, T> hgetAllProto(final String key, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseBytesToProtos(hgetAll(key), protoClass);
	}

	Map<String, String> hgetAllString(final String key) throws FedisException {
		return FedisUtils.safeParseBytesToStrings(hgetAll(key));
	}
	
	Map<String, Integer> hgetAllInteger(final String key) throws FedisException {
		return FedisUtils.safeParseBytesToIntegers(hgetAll(key));
	}
	
	Map<byte[], byte[]> hgetAll(final String key) throws FedisException {
		final ActionResult<Map<byte[], byte[]>> result = new ActionResult<Map<byte[], byte[]>>();

		redisProxy.runCommand(key, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hgetAll(SafeEncoder.encode(key)));
				
			}
		});

		return result.getValue();
	}
	



	Long hincrBy(final String key, final String field, final Long offset) throws FedisException {
		FedisUtils.checkNull(field);
		FedisUtils.checkNull(offset);
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hincrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), offset));
			}
		});

		return result.getValue();
	}
	
	
	Double hincrBy(final String key, final String field, final Float offset) throws FedisException {
		FedisUtils.checkNull(field);
		FedisUtils.checkNull(offset);
		final ActionResult<Double> result = new ActionResult<Double>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hincrByFloat(SafeEncoder.encode(key), SafeEncoder.encode(field), offset));
			}
		});

		return result.getValue();
	}
	

	Set<String> hkeys(final String key) throws FedisException {
		final ActionResult<Set<byte[]>> result = new ActionResult<Set<byte[]>>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hkeys(SafeEncoder.encode(key)));
			}
		});

		return FedisUtils.safeParseBytesToStrings(result.getValue());
	}

	Long hlen(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hlen(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}
	



	<T extends ProtoEntity> List<T> hmgetProtos(final String key, final Class<T> protoClass , String... fields) throws FedisException {
		FedisUtils.checkNull(fields);
		byte[][] fieldsBytes = FedisUtils.safeParseStringsToBytes(fields).toArray(new byte[][] {});
		return FedisUtils.safeParseBytesToProtos(hmget(key , fieldsBytes), protoClass);
	}
	
	List<String> hmgetStrings(final String key , String... fields) throws FedisException {
		FedisUtils.checkNull(fields);
		byte[][] fieldsBytes = FedisUtils.safeParseStringsToBytes(fields).toArray(new byte[][] {});
		return FedisUtils.safeParseBytesToStrings(hmget(key , fieldsBytes));
	}
	
	List<Integer> hmgetIntegers(final String key, String... fields) throws FedisException {
		FedisUtils.checkNull(fields);
		byte[][] fieldsBytes = FedisUtils.safeParseStringsToBytes(fields).toArray(new byte[][] {});
		return FedisUtils.safeParseBytesToIntegers(hmget(key , fieldsBytes));
	}
	
	List<byte[]> hmget(final String key, final byte[]... fields) throws FedisException {
		FedisUtils.checkNull(fields);
		final ActionResult<List<byte[]>> result = new ActionResult<List<byte[]>>();

		redisProxy.runCommand(key, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hmget(SafeEncoder.encode(key), fields));
			}
		});
		return result.getValue();
	}
	

	String hmsetProto(final String key, final Map<String, ProtoEntity> hashValue) throws FedisException {
		FedisUtils.checkNull(hashValue);
		return hmset(key , FedisUtils.safeParseProtosToBytes(hashValue));
	}
	
	String hmsetString(final String key, final Map<String, String> hashValue) throws FedisException {
		FedisUtils.checkNull(hashValue);
		return hmset(key , FedisUtils.safeParseStringsToBytes(hashValue));
	}
	
	String hmsetInteger(final String key, final Map<String, Integer> hashValue) throws FedisException {
		FedisUtils.checkNull(hashValue);
		return hmset(key , FedisUtils.safeParseIntegersToBytes(hashValue));
	}

	String hmset(final String key, final Map<byte[], byte[]> hashValue) throws FedisException {
		FedisUtils.checkNull(hashValue);
		final ActionResult<String> result = new ActionResult<String>();


		redisProxy.runCommand(key, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hmset(SafeEncoder.encode(key), hashValue));
			}
		});

		return result.getValue();
	}
	

	Long hsetProto(final String key, final String field, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return hset(key , field , value.toByteArray());
	}

	Long hsetString(final String key, final String field, final String value) throws FedisException {
		return hset(key , field , SafeEncoder.encode(value));
	}

	Long hsetInteger(final String key, final String field, final Integer value) throws FedisException {
		FedisUtils.checkNull(value);
		return hset(key , field , SafeEncoder.encode(String.valueOf(value)));
	}

	Long hsetInteger(final String key, final Integer field, final Integer value) throws FedisException {
		FedisUtils.checkNull(field);
		FedisUtils.checkNull(value);
		return hset(key , String.valueOf(field) , SafeEncoder.encode(String.valueOf(value)));
	}
	
	Long hsetLong(final String key, final Integer field, final Long value) throws FedisException {
		FedisUtils.checkNull(field);
		FedisUtils.checkNull(value);
		return hset(key , String.valueOf(field) , SafeEncoder.encode(String.valueOf(value)));
	}
	
	Long hset(final String key, final String field, final byte[] value) throws FedisException {
		FedisUtils.checkNull(value);
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hset(SafeEncoder.encode(key), SafeEncoder.encode(field), value));
			}
		});

		return result.getValue();
	}

	Long hsetNxProto(final String key, final String field, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return hsetNx(key , field , value.toByteArray());
	}

	Long hsetNxString(final String key, final String field, final String value) throws FedisException {
		return hsetNx(key , field , SafeEncoder.encode(value));
	}

	Long hsetNxInteger(final String key, final String field, final Integer value) throws FedisException {
		FedisUtils.checkNull(value);
		return hsetNx(key , field , SafeEncoder.encode(String.valueOf(value)));
	}

	Long hsetNxInteger(final String key, final Integer field, final Integer value) throws FedisException {
		FedisUtils.checkNull(field);
		FedisUtils.checkNull(value);
		return hsetNx(key , String.valueOf(field) , SafeEncoder.encode(String.valueOf(value)));
	}
	
	Long hsetNxLong(final String key, final Integer field, final Long value) throws FedisException {
		FedisUtils.checkNull(field);
		FedisUtils.checkNull(value);
		return hsetNx(key , String.valueOf(field) , SafeEncoder.encode(String.valueOf(value)));
	}
	
	Long hsetNx(final String key, final String field, final byte[] value) throws FedisException {
		FedisUtils.checkNull(value);
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hsetnx(SafeEncoder.encode(key), SafeEncoder.encode(field), value));
			}
		});

		return result.getValue();
	}
	

	<T extends ProtoEntity> List<T> hvalsProto(final String key, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseBytesToProtos(hvals(key) , protoClass);
	}

	List<Integer> hvalsInteger(final String key) throws FedisException {
		return FedisUtils.safeParseBytesToIntegers(hvals(key));
	}

	List<String> hvalsString(final String key) throws FedisException {
		return FedisUtils.safeParseBytesToStrings(hvals(key));
	}
	
	List<byte[]> hvals(final String key) throws FedisException {

		final ActionResult<List<byte[]>> result = new ActionResult<List<byte[]>>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.hvals(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

}
