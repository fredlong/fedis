package com.fedis.impl;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Jedis;
import redis.clients.util.SafeEncoder;

import com.fedis.protobuff.ProtoEntity;
import com.fedis.util.Action;
import com.fedis.util.ActionResult;
import com.fedis.util.FedisException;
import com.fedis.util.FedisUtils;

class StringCommandImpl extends FedisCommandImpl{

	StringCommandImpl(RedisProxy redisProxy) {
		super(redisProxy);
	}
	
	Long append(final String key, final String afterString) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.append(SafeEncoder.encode(key), SafeEncoder.encode(afterString)));
			}
		});

		return result.getValue();
	}
	
	
	Long bitcount(final String key) throws FedisException {
		return bitcount(key , 0 , -1);
	}
	
	Long bitcount(final String key, final long start , final long end) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.bitcount(key, start, end));
			}
		});

		return result.getValue();
	}
	
	

	/**
	 * 所有的key中的值应该是integer
	 * @param destKey
	 * @param srcKeys
	 * @return 返回合并之后的写入destKey中的值
	 * @throws FedisException
	 */
	Long bitop(final BitOP op , final String destKey , final String... srcKeys) throws FedisException{
		FedisUtils.checkNull(srcKeys);
		List<Long> values = new ArrayList<Long>();
		for(int i = 0 ; i< srcKeys.length ; i++){
			values.add(getLong(srcKeys[i]));
		}
		
		if(values.size() == 0){
			setLong(destKey , 0L);
			return 0L;
		}
		
		Long result = values.get(0);
		
		if(BitOP.NOT == op){
			result = ~result;
		}else{
			for(int i = 1 ; i < values.size() ; i++){
				switch(op){
					case AND:
						result = result & values.get(i);
						break;
					case OR:
						result = result | values.get(i);
						break;
					case XOR:
						result = result ^ values.get(i);
						break;
				default:
					break;
				}
			}

		}
		
		setLong(destKey , result);
		return result;
	}
	
	Long bitpos(final String key , final boolean bit , final BitPosParams param) throws FedisException{
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.bitpos(SafeEncoder.encode(key), bit, param));
			}
		});

		return result.getValue();
	}
	
	Long bitpos(final String key , boolean bit) throws FedisException{
		return bitpos(key , bit , new BitPosParams(0));
	}
	
	Long decr(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.decr(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

	Long decrBy(final String key, final long offset) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.decrBy(SafeEncoder.encode(key), offset));
			}
		});

		return result.getValue();
	}
	


	<T extends ProtoEntity> T getProto(final String key, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseProto(get(key), protoClass);

	}

	String getString(final String key) throws FedisException {
		return FedisUtils.safeParseString(get(key));
	}
	

	Integer getInteger(final String key) throws FedisException {
		return FedisUtils.safeParseInteger(get(key));
	}
	
	Long getLong(final String key) throws FedisException {
		return FedisUtils.safeParseLong(get(key));
	}
	
	byte[] get(final String key) throws FedisException {
		final ActionResult<byte[]> result = new ActionResult<byte[]>();

		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.get(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}
	
	
	Boolean getbit(final String key , final Long offset) throws FedisException{
		final ActionResult<Boolean> result = new ActionResult<Boolean>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.getbit(SafeEncoder.encode(key) , offset));
			}
		});

		return result.getValue();
	}
	

	
	
	
	
	
	String getRange(final String key , final long startOffset , final long endOffset) throws FedisException{
		final ActionResult<byte[]> result = new ActionResult<byte[]>();

		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.getrange(SafeEncoder.encode(key), startOffset, endOffset));
			}
		});

		return FedisUtils.safeParseString(result.getValue());
	}

	
	
	
	<T extends ProtoEntity> T getSetProto(final String key, final Class<T> protoClass, final ProtoEntity newValue) throws FedisException {
		FedisUtils.checkNull(newValue);
		return FedisUtils.safeParseProto(getSet(key , newValue.toByteArray()), protoClass);
	}
	
	String getSetString(final String key , final String newValue) throws FedisException{
		return FedisUtils.safeParseString(getSet(key , SafeEncoder.encode(newValue)));
	}
	
	Integer getSetInteger(final String key , final Integer newValue) throws FedisException{
		FedisUtils.checkNull(newValue);
		return FedisUtils.safeParseInteger(getSet(key , SafeEncoder.encode(String.valueOf(newValue))));
	}
	
	
	byte[] getSet(final String key, final byte[] newValue) throws FedisException{
		FedisUtils.checkNull(newValue);
		final ActionResult<byte[]> result = new ActionResult<byte[]>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.getSet(SafeEncoder.encode(key), newValue));

			}
		});

		return result.getValue();
	}
	
	Long incr(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.incr(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

	Long incrBy(final String key, final Long offset) throws FedisException {
		FedisUtils.checkNull(offset);
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.incrBy(SafeEncoder.encode(key), offset));
			}
		});
		return result.getValue();
	}
	
	Double incrByFloat(final String key, final Float offset) throws FedisException {
		FedisUtils.checkNull(offset);
		final ActionResult<Double> result = new ActionResult<Double>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.incrByFloat(SafeEncoder.encode(key), offset));
			}
		});
		return result.getValue();
	}
	
	String pSetExProto(final String key, final int milliSeconds, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return setEx(key , milliSeconds , value.toByteArray());
	}

	String pSetExString(final String key, final int milliSeconds, final String strValue) throws FedisException {
		return setEx(key , milliSeconds , SafeEncoder.encode(strValue));
	}

	String pSetExInteger(final String key, final int milliSeconds, final Integer intValue) throws FedisException {
		FedisUtils.checkNull(intValue);
		return setEx(key , milliSeconds , SafeEncoder.encode(String.valueOf(intValue)));
	}
	
	String pSetEx(final String key, final int milliSeconds, final byte[] value) throws FedisException {
		final ActionResult<String> result = new ActionResult<String>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.psetex(SafeEncoder.encode(key), milliSeconds, value));
			}
		});

		return result.getValue();
	}
	
	String setProto(final String key, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return set(key , value.toByteArray());
	}

	String setString(final String key, final String value) throws FedisException {
		return set(key , SafeEncoder.encode(value));
	}
	
	String setInteger(final String key, final Integer value) throws FedisException {
		FedisUtils.checkNull(value);
		return set(key , SafeEncoder.encode(String.valueOf(value)));
	}
	
	String setLong(final String key, final Long value) throws FedisException {
		FedisUtils.checkNull(value);
		return set(key , SafeEncoder.encode(String.valueOf(value)));
	}
	
	String set(final String key , final byte[] value) throws FedisException{
		FedisUtils.checkNull(value);
		final ActionResult<String> result = new ActionResult<String>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.set(SafeEncoder.encode(key), value));
			}
		});

		return result.getValue();
	}
	
	Boolean setBit(final String key , final Long offset , final Boolean value) throws FedisException{
		FedisUtils.checkNull(offset);
		FedisUtils.checkNull(value);
		final ActionResult<Boolean> result = new ActionResult<Boolean>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.setbit(SafeEncoder.encode(key), offset, value));
			}
		});

		return result.getValue();
	}


	String setExProto(final String key, final int seconds, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return setEx(key , seconds , value.toByteArray());
	}

	String setExString(final String key, final int seconds, final String strValue) throws FedisException {
		return setEx(key , seconds , SafeEncoder.encode(strValue));
	}

	String setExInteger(final String key, final int seconds, final Integer intValue) throws FedisException {
		FedisUtils.checkNull(intValue);
		return setEx(key , seconds , SafeEncoder.encode(String.valueOf(intValue)));
	}
	
	String setEx(final String key, final int seconds, final byte[] value) throws FedisException {
		FedisUtils.checkNull(value);
		final ActionResult<String> result = new ActionResult<String>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.setex(SafeEncoder.encode(key), seconds, value));
			}
		});

		return result.getValue();
	}

	Long setNxProto(final String key, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return setNx(key , value.toByteArray());
	}

	Long setNxInteger(final String key, final int intValue) throws FedisException {
		FedisUtils.checkNull(intValue);
		return setNx(key , SafeEncoder.encode(String.valueOf(intValue)));
	}

	Long setNxString(final String key, final String strValue) throws FedisException {
		return setNx(key , SafeEncoder.encode(strValue));
	}
	
	Long setNx(final String key , final byte[] value) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.setnx(SafeEncoder.encode(key), value));
			}
		});

		return result.getValue();
	}
	
	Long setRange(final String key , final Long offset , final String value) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.setrange(key, offset, value));
			}
		});

		return result.getValue();
	}

	Long strlen(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.strlen(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}


	
	
	
	

}
