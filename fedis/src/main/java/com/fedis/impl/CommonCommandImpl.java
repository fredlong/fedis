package com.fedis.impl;

import redis.clients.jedis.Jedis;
import redis.clients.util.SafeEncoder;

import com.fedis.util.Action;
import com.fedis.util.ActionResult;
import com.fedis.util.FedisException;


class CommonCommandImpl extends FedisCommandImpl{

	CommonCommandImpl(RedisProxy redisProxy) {
		super(redisProxy);
	}
	
	/**
	 * 不再兼容原Redis中可以批量del的命令，因为不同的key会路由到不同的实例
	 * 
	 * @param key
	 * @return 删除成功返回1，否则返回0
	 */
	Long del(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.del(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

	Boolean exists(final String key) throws FedisException {
		final ActionResult<Boolean> result = new ActionResult<Boolean>();

		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.exists(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

	/**
	 * @param key
	 * @param seconds
	 * @return 1 成功设置了过期时间
	 *         <p>
	 *         0设置过期时间失败
	 */
	Long expire(final String key, final int seconds) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.expire(SafeEncoder.encode(key), seconds));
			}
		});

		return result.getValue();
	}

	/**
	 * @param key
	 * @param seconds
	 * @return 1 成功设置了过期时间
	 *         <p>
	 *         0设置过期时间失败
	 */
	Long expireAt(final String key, final int unixTimeStamp) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.expireAt(SafeEncoder.encode(key), unixTimeStamp));
			}
		});

		return result.getValue();
	}
	
	Long persist(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.persist(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}
	
	Long ttl(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.ttl(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

	
	void watch(final String key) throws FedisException{
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				jedis.watch(key);
			}
		});
	}
	
	void unwatch(final String key) throws FedisException{
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				jedis.unwatch();
			}
		});
	}

}
