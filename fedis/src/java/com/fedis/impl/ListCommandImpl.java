package com.fedis.impl;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.util.SafeEncoder;

import com.fedis.protobuff.ProtoEntity;
import com.fedis.util.Action;
import com.fedis.util.ActionResult;
import com.fedis.util.FedisException;
import com.fedis.util.FedisUtils;


class ListCommandImpl extends FedisCommandImpl{

	ListCommandImpl(RedisProxy redisProxy) {
		super(redisProxy);
	}

	<T extends ProtoEntity> T blpopProto(final String key , final Class<T> protoClass , final int timeout) throws FedisException{
		return FedisUtils.safeParseProto(blpop(key , timeout), protoClass);
	}
	
	Integer blpopInteger(final String key , final int timeout) throws FedisException{
		return FedisUtils.safeParseInteger(blpop(key , timeout));
	}
	
	String blpopString(final String key , final int timeout) throws FedisException{
		return FedisUtils.safeParseString(blpop(key , timeout));
		
	}
	
	
	/**
	 * 由于本组件支持集群，所以目前只支持从一个List中blpop数据
	 * @param key
	 * @param timeout
	 * @return 从指定List中读取出来的值
	 * @throws FedisException
	 */
	byte[] blpop(final String key , final int timeout) throws FedisException{
		final ActionResult<byte[]> result = new ActionResult<byte[]>();
		redisProxy.runCommand(key, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				List<byte[]> buffer = jedis.blpop(timeout, SafeEncoder.encode(key));
				if(buffer.size() > 1){
					result.setValue(buffer.get(1));
				}
				
			}
		});

		return result.getValue();
	}
	
	
	<T extends ProtoEntity> T brpopProto(final String key , final Class<T> protoClass , final int timeout) throws FedisException{
		return FedisUtils.safeParseProto(brpop(key , timeout), protoClass);
	}
	
	Integer brpopInteger(final String key , final int timeout) throws FedisException{
		return FedisUtils.safeParseInteger(brpop(key , timeout));
	}
	
	String brpopString(final String key , final int timeout) throws FedisException{
		return FedisUtils.safeParseString(brpop(key , timeout));
	}
	
	
	/**
	 * 由于本组件支持集群，所以目前只支持从一个List中blpop数据
	 * @param key
	 * @param timeout
	 * @return 从指定List中读取出来的值
	 * @throws FedisException
	 */
	byte[] brpop(final String key , final int timeout) throws FedisException{
		final ActionResult<byte[]> result = new ActionResult<byte[]>();
		redisProxy.runCommand(key, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				List<byte[]> buffer = jedis.brpop(timeout, SafeEncoder.encode(key));
				if(buffer.size() > 1){
					result.setValue(buffer.get(1));
				}
				
			}
		});

		return result.getValue();
	}
	
	
	<T extends ProtoEntity> T brpoplpushProto(final String sourceKey , final String destKey , final Class<T> protoClass , final int timeout) throws FedisException{
		return FedisUtils.safeParseProto(brpoplpush(sourceKey , destKey , timeout), protoClass);
	}
	
	Integer brpoplpushInteger(final String sourceKey , final String destKey , final int timeout) throws FedisException{
		return FedisUtils.safeParseInteger(brpoplpush(sourceKey , destKey , timeout));
	}
	
	String brpoplpushString(final String sourceKey , final String destKey , final int timeout) throws FedisException{
		return FedisUtils.safeParseString(brpoplpush(sourceKey , destKey , timeout));
	}
	
	/**
	 * 由于本组件支持集群，所以目前只支持从一个List中blpop数据
	 * @param key
	 * @param timeout
	 * @return 从指定List中读取出来的值
	 * @throws FedisException
	 */
	byte[] brpoplpush(final String sourceKey , final String destKey , final int timeout) throws FedisException{
		final ActionResult<byte[]> result = new ActionResult<byte[]>();
		redisProxy.runCommand(sourceKey, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				List<byte[]> buffer = jedis.brpop(timeout, SafeEncoder.encode(sourceKey));
				if(buffer.size() > 1){
					result.setValue(buffer.get(1));
				}
				
			}
		});
		
		if(null != result.getValue())
		{
			lpush(destKey , result.getValue());
		}
		
		return result.getValue();
	}
	
	<T extends ProtoEntity> T lindexProto(final String key, final int index, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseProto(lindex(key , index) , protoClass);
	}

	String lindexString(final String key, final int index) throws FedisException {
		return FedisUtils.safeParseString(lindex(key , index));
	}
	
	Integer lindexInteger(final String key, final int index) throws FedisException {
		return FedisUtils.safeParseInteger(lindex(key , index));
	}
	
	byte[] lindex(final String key, final int index) throws FedisException {
		final ActionResult<byte[]> result = new ActionResult<byte[]>();

		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {

				byte[] buffer = jedis.lindex(SafeEncoder.encode(key), index);
				result.setValue(buffer);
			}
		});

		return result.getValue();
	}
	

	Long linsertProto(final String key, final LIST_POSITION position, final ProtoEntity pivot, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(pivot);
		FedisUtils.checkNull(value);
		return linsert(key, position, pivot.toByteArray() , value.toByteArray());
	}

	Long linsertInteger(final String key, final LIST_POSITION position, final Integer pivot, final int value) throws FedisException {
		FedisUtils.checkNull(pivot);
		FedisUtils.checkNull(value);
		return linsert(key, position, SafeEncoder.encode(String.valueOf(pivot)), SafeEncoder.encode(String.valueOf(value)));
	}

	Long linsertString(final String key, final LIST_POSITION position, final String pivot, final String value) throws FedisException {
		return linsert(key, position, SafeEncoder.encode(pivot), SafeEncoder.encode(value));
	}

	Long linsert(final String key, final LIST_POSITION position, final byte[] pivot, final byte[] value) throws FedisException {
		FedisUtils.checkNull(pivot);
		FedisUtils.checkNull(value);
		
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.linsert(SafeEncoder.encode(key), position, pivot , value));
			}
		});

		return result.getValue();
	}

	Long llen(final String key) throws FedisException {
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.llen(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

	<T extends ProtoEntity> T lpopProto(final String key, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseProto(lpop(key) , protoClass);
	}
	
	Integer lpopInteger(final String key) throws FedisException {
		return FedisUtils.safeParseInteger(lpop(key));
	}

	String lpopString(final String key) throws FedisException {
		return FedisUtils.safeParseString(lpop(key));
	}

	byte[] lpop(final String key) throws FedisException {
		final ActionResult<byte[]> result = new ActionResult<byte[]>();
		redisProxy.runCommand(key, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.lpop(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}

	Long lpushProtos(final String key, final ProtoEntity... values) throws FedisException {
		FedisUtils.checkNull(values);
		return lpush(key, FedisUtils.safeParseProtosToBytes(values).toArray(new byte[][] {}));
	}

	Long lpushStrings(final String key, final String... values) throws FedisException {
		FedisUtils.checkNull(values);
		return lpush(key, FedisUtils.safeParseStringsToBytes(values).toArray(new byte[][] {}));
	}

	Long lpushIntegers(final String key, final Integer... values) throws FedisException {
		FedisUtils.checkNull(values);
		return lpush(key, FedisUtils.safeParseIntegersToBytes(values).toArray(new byte[][] {}));
	}

	Long lpush(final String key, final byte[]... values) throws FedisException {
		FedisUtils.checkNull(values);
		
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.lpush(SafeEncoder.encode(key), values));
			}
		});

		return result.getValue();
	}
	
	Long lpushxProto(final String key, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return lpushx(key, value.toByteArray());
	}
	Long lpushxString(final String key, final String value) throws FedisException {
		return lpushx(key, SafeEncoder.encode(value));
	}
	Long lpushxInteger(final String key, final int value) throws FedisException {
		FedisUtils.checkNull(value);
		return lpushx(key, SafeEncoder.encode(String.valueOf(value)));
	}
	Long lpushx(final String key, final byte[] value) throws FedisException {
		FedisUtils.checkNull(value);
		
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.lpushx(SafeEncoder.encode(key), value));
			}
		});

		return result.getValue();
	}


	<T extends ProtoEntity> List<T> lrangeProto(final String key, final int start, final int end, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseBytesToProtos(lrange(key , start , end) , protoClass);
	}
	List<Integer> lrangeInteger(final String key, final int start, final int end) throws FedisException {
		return FedisUtils.safeParseBytesToIntegers(lrange(key , start ,end));
	}
	List<String> lrangeString(final String key, final int start, final int end) throws FedisException {
		return FedisUtils.safeParseBytesToStrings(lrange(key , start ,end));
	}
	List<byte[]> lrange(final String key, final int start, final int end) throws FedisException {
		final ActionResult<List<byte[]>> result = new ActionResult<List<byte[]>>();

		redisProxy.runCommand(key, new Action<Jedis>() {
			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.lrange(SafeEncoder.encode(key), start, end));
			}
		});

		return result.getValue();
	}
	
	
	Long lremInteger(final String key, final int count, final Integer value) throws FedisException {
		FedisUtils.checkNull(value);
		return lrem(key, count, SafeEncoder.encode(String.valueOf(value)));
	}

	Long lremString(final String key, final int count, final String value) throws FedisException {
		return lrem(key, count, SafeEncoder.encode(value));
	}

	Long lremProto(final String key, final int count, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return lrem(key, count, value.toByteArray());
	}

	private Long lrem(final String key, final int count, final byte[] value) throws FedisException {
		FedisUtils.checkNull(value);
		
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.lrem(SafeEncoder.encode(key), count, value));
			}
		});

		return result.getValue();
	}
	

	String lsetProto(final String key, final int index, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return lset(key, index, value.toByteArray());
	}

	String lsetString(final String key, final int index, final String value) throws FedisException {
		return lset(key, index, SafeEncoder.encode(value));
	}

	String lsetInteger(final String key, final int index, final Integer value) throws FedisException {
		FedisUtils.checkNull(value);
		return lset(key, index, SafeEncoder.encode(String.valueOf(value)));
	}

	String lset(final String key, final int index, final byte[] value) throws FedisException {
		FedisUtils.checkNull(value);
		
		final ActionResult<String> result = new ActionResult<String>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.lset(SafeEncoder.encode(key), index, value));
			}
		});

		return result.getValue();
	}
	
	
	String ltrim(final String key, final int start, final int end) throws FedisException {
		final ActionResult<String> result = new ActionResult<String>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.ltrim(SafeEncoder.encode(key), start, end));
			}
		});

		return result.getValue();
	}
	
	
	<T extends ProtoEntity> T rpopProto(final String key, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseProto(rpop(key), protoClass);
	}

	Integer rpopInteger(final String key) throws FedisException {
		return FedisUtils.safeParseInteger(rpop(key));
	}

	String rpopString(final String key) throws FedisException {
		return FedisUtils.safeParseString(rpop(key));
	}
	
	byte[] rpop(final String key) throws FedisException {
		final ActionResult<byte[]> result = new ActionResult<byte[]>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.rpop(SafeEncoder.encode(key)));
			}
		});

		return result.getValue();
	}
	
	
	
	<T extends ProtoEntity> T rpoplpushProto(final String sourceKey , final String destKey, final Class<T> protoClass) throws FedisException {
		return FedisUtils.safeParseProto(rpoplpush(sourceKey , destKey), protoClass);
	}

	Integer rpoplpushInteger(final String sourceKey , final String destKey) throws FedisException {
		return FedisUtils.safeParseInteger(rpoplpush(sourceKey , destKey));
	}

	String rpoplpushString(final String sourceKey , final String destKey) throws FedisException {
		return FedisUtils.safeParseString(rpoplpush(sourceKey , destKey));

	}
	
	byte[] rpoplpush(final String sourceKey , final String destKey) throws FedisException {
		byte[] value =rpop(sourceKey);
		if(null == value){
			return null;
		}
		lpush(destKey , value);
		return value;
	}
	
	
	
	Long rpushProtos(final String key, final ProtoEntity... values) throws FedisException {
		return rpush(key, FedisUtils.safeParseProtosToBytes(values).toArray(new byte[][] {}));
	}
	Long rpushStrings(final String key, final String... values) throws FedisException {
		return rpush(key, FedisUtils.safeParseStringsToBytes(values).toArray(new byte[][] {}));
	}
	Long rpushIntegers(final String key, final Integer... values) throws FedisException {
		return rpush(key, FedisUtils.safeParseIntegersToBytes(values).toArray(new byte[][] {}));
	}
	Long rpush(final String key, final byte[]... values) throws FedisException {
		FedisUtils.checkNull(values);
		
		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.rpush(SafeEncoder.encode(key), values));
			}
		});

		return result.getValue();
	}


	Long rpushxProto(final String key, final ProtoEntity value) throws FedisException {
		FedisUtils.checkNull(value);
		return rpushx(key, value.toByteArray());
	}
	Long rpushxString(final String key, final String value) throws FedisException {
		return rpushx(key, SafeEncoder.encode(value));
	}
	Long rpushxInteger(final String key, final Integer value) throws FedisException {
		return rpushx(key, SafeEncoder.encode(String.valueOf(value)));
	}

	Long rpushx(final String key, final byte[] value) throws FedisException {
		FedisUtils.checkNull(value);

		final ActionResult<Long> result = new ActionResult<Long>();
		redisProxy.runCommand(key, new Action<Jedis>() {

			@Override
			public void run(Jedis jedis) {
				result.setValue(jedis.rpushx(SafeEncoder.encode(key), value));
			}
		});

		return result.getValue();
	}
}
