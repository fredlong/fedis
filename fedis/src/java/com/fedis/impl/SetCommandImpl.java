package com.fedis.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

import com.fedis.util.Action;
import com.fedis.util.ActionResult;
import com.fedis.util.FedisException;


class SetCommandImpl  extends FedisCommandImpl{

	SetCommandImpl(RedisProxy redisProxy) {
		super(redisProxy);
	}

	Long sadd(final String key, final String value) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.sadd(key, value));
			}
		});

		return result.getValue();
	}

	Long sadd(final String key, final Integer value) throws FedisException {
		return sadd(key, String.valueOf(value));
	}

	Long srem(final String key, final String value) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.srem(key, value));
			}
		});

		return result.getValue();
	}

	Set<String> smembers(final String key) throws FedisException {
		final ActionResult<Set<String>> result = new ActionResult<Set<String>>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.smembers(key));
			}
		});

		return result.getValue();
	}

	ScanResult<Integer> sscanInteger(final String key, final String cursor) throws FedisException {
		final ActionResult<ScanResult<Integer>> result = new ActionResult<ScanResult<Integer>>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				ScanResult<String> returnValue = jedis.sscan(key, String.valueOf(cursor));
				List<Integer> integerList = new ArrayList<Integer>();
				for (String s : returnValue.getResult()) {
					integerList.add(Integer.valueOf(s));
				}

				result.setValue(new ScanResult<Integer>(returnValue.getCursorAsBytes(), integerList));
			}
		});

		return result.getValue();
	}
	
	ScanResult<String> sscan(final String key, final String cursor) throws FedisException {
		final ActionResult<ScanResult<String>> result = new ActionResult<ScanResult<String>>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.sscan(key, String.valueOf(cursor)));
			}
		});

		return result.getValue();
	}

	Long scard(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.scard(key));
			}
		});

		return result.getValue();
	}

	Set<Integer> smembersInteger(final String key) throws FedisException {
		Set<Integer> result = new HashSet<Integer>();
		Set<String> set = smembers(key);
		Iterator<String> it = set.iterator();
		while (it.hasNext()) {
			result.add(Integer.valueOf(it.next()));
		}
		return result;
	}

	boolean sismember(final String key, final String member) throws FedisException {
		final ActionResult<Boolean> result = new ActionResult<Boolean>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.sismember(key, member));
			}
		});

		return result.getValue();
	}

	boolean sismemberInteger(final String key, final Integer member) throws FedisException {
		return sismember(key, String.valueOf(member));
	}

	Long srem(final String key, final Integer value) throws FedisException {
		return srem(key, String.valueOf(value));
	}
	
}
