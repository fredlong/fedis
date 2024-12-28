package com.fedis.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.util.SafeEncoder;

import com.fedis.RedisCluster;
import com.fedis.protobuff.ProtoEntity;
import com.fedis.router.ConsistentHashRouter;
import com.fedis.router.GeneralHashHelper;
import com.fedis.router.Router;
import com.fedis.util.Action;
import com.fedis.util.ActionResult;
import com.fedis.util.FedisException;
import com.fedis.util.FedisUtils;



public class RedisProxy {
	private Router<RedisCluster> jedisGroup = null;
	private static int maxRetryTimes = 3;
	String groupName =  FedisUtils.EMPTY_STRING;
	StringCommandImpl sci;
	HashCommandImpl hci;
	CommonCommandImpl cci;
	SetCommandImpl setci;
	SortedSetCommandImpl ssci;
	ListCommandImpl lci;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisProxy.class);
	
	
	public RedisProxy(String redisGroupName) {
		this.groupName = redisGroupName;
		jedisGroup = new ConsistentHashRouter<RedisCluster>(new GeneralHashHelper());
		
		sci = new StringCommandImpl(this);
		hci = new HashCommandImpl(this);
		cci = new CommonCommandImpl(this);
		setci = new SetCommandImpl(this);
		ssci = new SortedSetCommandImpl(this);
		lci = new ListCommandImpl(this);
	}

	public String getGroupName() {
		return groupName;
	}

	public void initNode(RedisCluster jedisPool) throws FedisException {
		jedisGroup.initNode(jedisPool, jedisPool.getRedisNode().getWeight());
	}
	

	protected void runCommand(String key, Action<Jedis> command) throws FedisException {
		runCommand(key, command, 1);
	}

	protected void runCommand(String key, Action<Jedis> command, int tryTimes) throws FedisException {
		JedisPool jedisPool = null;
		Jedis jedis = null;

		try {
			jedisPool = this.jedisGroup.get(key).getJedisPool();

			jedis = jedisPool.getResource();
			command.run(jedis);
			jedisPool.returnResource(jedis);

		} catch (Exception e) {
			if (jedisPool != null && jedis != null) {
				jedisPool.returnBrokenResource(jedis);
			}

			LOGGER.error(String.format("RedisProxy_%s runCommand error:%s,tryTimes:%s", this.groupName, e.getMessage(), tryTimes), e);
			if (tryTimes < maxRetryTimes) {
				runCommand(key, command, tryTimes + 1);
			} else {
				throw new FedisException(String.format("RedisProxy_%s runCommand error:%s", this.groupName, e.getMessage()), e);
			}
		}
	}
	
	public RedisCluster getCurrentRedisCluster(String key) throws FedisException {
		return this.jedisGroup.get(key);
	}

	public Router<RedisCluster> getCurrentRouter() {
		return this.jedisGroup;
	}
	
	
	/*
	 * Common
	 */
	
	public Long del(final String key) throws FedisException {
		return cci.del(key);
	}

	public Boolean exists(final String key) throws FedisException {
		return cci.exists(key);
	}

	public Long expire(final String key, final int seconds) throws FedisException {
		return cci.expire(key, seconds);
	}

	public Long expireAt(final String key, final int unixTimeStamp) throws FedisException {
		return cci.expireAt(key, unixTimeStamp);
	}
	
	public Long persist(final String key) throws FedisException {
		return cci.persist(key);
	}
	
	public Long ttl(final String key) throws FedisException {
		return cci.ttl(key);
	}

	public void watch(final String key) throws FedisException{
		cci.watch(key);
	}
	
	public void unwatch(final String key) throws FedisException{
		cci.unwatch(key);
	}

	
	/*
	 * String
	 */
	
	public Long append(final String key, final String afterString) throws FedisException {
		return sci.append(key, afterString);
	}

	public Long decr(final String key) throws FedisException {
		return sci.decr(key);
	}

	public Long decrBy(final String key, final long offset) throws FedisException {
		return sci.decrBy(key, offset);
	}
	
	public Long incr(final String key) throws FedisException {
		return sci.incr(key);
	}

	public Long incrBy(final String key, final long offset) throws FedisException {
		return sci.incrBy(key, offset);
	}
	
	public <T extends ProtoEntity> T getProto(final String key, final Class<T> protoClass) throws FedisException {
		return sci.getProto(key, protoClass);
	}

	public String getString(final String key) throws FedisException {
		return sci.getString(key);
	}
	
	public Integer getInteger(final String key) throws FedisException {
		return sci.getInteger(key);
	}

	public byte[] get(final String key) throws FedisException {
		return sci.get(key);
	}
	
	public String set(final String key, final int intValue) throws FedisException {
		return sci.setInteger(key, intValue);
	}

	public String setExProto(final String key, final int seconds, final ProtoEntity value) throws FedisException {
		return sci.setExProto(key, seconds, value);
	}

	public String setExString(final String key, final int seconds, final String strValue) throws FedisException {
		return sci.setExString(key, seconds, strValue);
	}

	public String setExInteger(final String key, final int seconds, final int intValue) throws FedisException {
		return sci.setExInteger(key, seconds, intValue);
	}

	public Long setnxProto(final String key, final ProtoEntity value) throws FedisException {
		return sci.setNxProto(key, value);
	}

	public Long setnxInteger(final String key, final int intValue) throws FedisException {
		return sci.setNxInteger(key, intValue);
	}

	public Long setnxString(final String key, final String strValue) throws FedisException {
		return sci.setNxString(key, strValue);
	}

	public Long strlen(final String key) throws FedisException {
		return sci.strlen(key);
	}

	public String setProto(final String key, final ProtoEntity value) throws FedisException {
		return sci.setProto(key, value);
	}

	public String setString(final String key, final String value) throws FedisException {
		return sci.setString(key, value);
	}
	
	public String setInteger(final String key, final Integer value) throws FedisException {
		return sci.setInteger(key, value);
	}
	
	public String set(final String key , final byte[] value) throws FedisException{
		return sci.set(key, value);
	}
	
	public <T extends ProtoEntity> T getSetProto(final String key, final Class<T> protoClass, final ProtoEntity newValue) throws FedisException {
		return sci.getSetProto(key, protoClass, newValue);
	}
	
	public String getSetString(final String key , final String newValue) throws FedisException{
		return sci.getSetString(key, newValue);
	}
	
	public Integer getSetInteger(final String key , final Integer newValue) throws FedisException{
		return sci.getSetInteger(key, newValue);
	}
	
	public byte[] getSet(final String key, final byte[] newValue) throws FedisException{
		return sci.getSet(key, newValue);
	}
	
	
	
	
	
	
	/*
	 * Hashes
	 */
	
	/**
	 * 返回列表里的元素的索引 index 存储在 key 里面。 下标是从0开始索引的，所以 0 是表示第一个元素， 1 表示第二个元素，并以此类推。
	 * 负数索引用于指定从列表尾部开始索引的元素。在这种方法下，-1 表示最后一个元素，-2 表示倒数第二个元素，并以此往前推。
	 * <p>
	 * 当 key 位置的值不是一个列表的时候，会返回一个error。
	 * 
	 * @param key
	 * @param index
	 * @param protoClass
	 * @return 请求的对应元素，或者当 index 超过范围的时候返回 null。
	 */
	public <T extends ProtoEntity> T lindexProto(final String key, final int index, final Class<T> protoClass) throws FedisException {
		return lci.lindexProto(key, index, protoClass);
	}

	/**
	 * 返回列表里的元素的索引 index 存储在 key 里面。 下标是从0开始索引的，所以 0 是表示第一个元素， 1 表示第二个元素，并以此类推。
	 * 负数索引用于指定从列表尾部开始索引的元素。在这种方法下，-1 表示最后一个元素，-2 表示倒数第二个元素，并以此往前推。
	 * <p>
	 * 当 key 位置的值不是一个列表的时候，会返回一个error。
	 * 
	 * @param key
	 * @param index
	 * @param protoClass
	 * @return 请求的对应元素，或者当 index 超过范围的时候返回 null。
	 */
	Integer lindexInteger(final String key, final int index) throws FedisException {
		Integer result = null;
		String resultStr = lindexString(key,index);
		if(resultStr != null)
		{
			result = Integer.parseInt(resultStr);
		}
		return result;
	}

	/**
	 * 返回列表里的元素的索引 index 存储在 key 里面。 下标是从0开始索引的，所以 0 是表示第一个元素， 1 表示第二个元素，并以此类推。
	 * 负数索引用于指定从列表尾部开始索引的元素。在这种方法下，-1 表示最后一个元素，-2 表示倒数第二个元素，并以此往前推。
	 * <p>
	 * 当 key 位置的值不是一个列表的时候，会返回一个error。
	 * 
	 * @param key
	 * @param index
	 * @param protoClass
	 * @return 请求的对应元素，或者当 index 超过范围的时候返回 null。
	 */
	public String lindexString(final String key, final int index) throws FedisException {
		return lci.lindexString(key, index);
	}

	public Long linsertProto(final String key, final LIST_POSITION position, final ProtoEntity pivot, final ProtoEntity value) throws FedisException {
		return lci.linsertProto(key, position, pivot, value);
	}

	public Long linsertInteger(final String key, final LIST_POSITION position, final Integer pivot, final Integer value) throws FedisException {
		return lci.linsertInteger(key, position, pivot, value);
	}

	public Long linsertString(final String key, final LIST_POSITION position, final String pivot, final String value) throws FedisException {
		return lci.linsertString(key, position, pivot, value);
	}

	public Long linsert(final String key, final LIST_POSITION position, final byte[] pivot, final byte[] value) throws FedisException {
		return lci.linsert(key, position, pivot, value);
	}

	public Long llen(final String key) throws FedisException {
		return lci.llen(key);
	}

	public <T extends ProtoEntity> T lpopProto(final String key, final Class<T> protoClass) throws FedisException {
		return lci.lpopProto(key, protoClass);
	}
	
	public Integer lpopInteger(final String key) throws FedisException {
		return lci.lpopInteger(key);
	}

	public String lpopString(final String key) throws FedisException {
		return lci.lpopString(key);
	}

	public byte[] lpop(final String key) throws FedisException {
		return lci.lpop(key);
	}

	
//	Long lpushProto(final String key, final ProtoEntity... values) throws FedisException {
//		List<byte[]> arrays = new ArrayList<byte[]>();
//		for (ProtoEntity value : values) {
//			arrays.add(value.toByteArray());
//		}
//		return lpush(key, arrays.toArray(new byte[][] {}));
//	}
//
//	Long lpushStr(final String key, final String... values) throws FedisException {
//		List<byte[]> arrays = new ArrayList<byte[]>();
//		for (String value : values) {
//			arrays.add(SafeEncoder.encode(value));
//		}
//		return lpush(key, arrays.toArray(new byte[][] {}));
//	}
//
//	Long lpushInteger(final String key, final int... values) throws FedisException {
//		List<byte[]> arrays = new ArrayList<byte[]>();
//		for (int value : values) {
//			arrays.add(SafeEncoder.encode(String.valueOf(value)));
//		}
//		return lpush(key, arrays.toArray(new byte[][] {}));
//	}

//	private Long lpush(final String key, final byte[]... values) throws FedisException {
//		final ActionResult<Long> result = new ActionResult<Long>();
//		redisProxy.runCommand(key, new Action<Jedis>() {
//
//			@Override
//			public void run(Jedis jedis) {
//				result.setValue(jedis.lpush(SafeEncoder.encode(key), values));
//			}
//		});
//
//		return result.getValue();
//	}
//
//	Long lpushx(final String key, final ProtoEntity value) throws FedisException {
//		return lpushx(key, value.toByteArray());
//	}
//
//	Long lpushx(final String key, final String value) throws FedisException {
//		return lci.lpushx
//	}

	public Long lpushx(final String key, final int value) throws FedisException {
		return lci.lpushxInteger(key, value);
	}
	
	
	
	public void rpush(final String key, final byte[] value) throws FedisException {
		//return lci.rpush(key, value);

	}

	/**
	 * 将sourcekey队列中的第一元素迁移到destKey的最后一个元素
	 * 
	 * @param sourceKey
	 * @param destKey
	 * @throws FedisException
	 */
	void lpoppush(final String sourceKey, final String destKey) throws FedisException {
		rpush(destKey, lpop(sourceKey));
	}


	

//	Long lrem(final String key, final int count, final int value) throws FedisException {
//		return lrem(key, count, SafeEncoder.encode(String.valueOf(value)));
//	}
//
//	Long lrem(final String key, final int count, final String value) throws FedisException {
//		return lrem(key, count, SafeEncoder.encode(value));
//	}
//
//	Long lrem(final String key, final int count, final ProtoEntity value) throws FedisException {
//		return lrem(key, count, value.toByteArray());
//	}

//	private Long lrem(final String key, final int count, final byte[] value) throws FedisException {
//		final ActionResult<Long> result = new ActionResult<Long>();
//		redisProxy.runCommand(key, new Action<Jedis>() {
//
//			@Override
//			public void run(Jedis jedis) {
//				result.setValue(jedis.lrem(SafeEncoder.encode(key), count, value));
//			}
//		});
//
//		return result.getValue();
//	}

	

	public Long lpushx(final String key, final byte[] value) throws FedisException {
		return lci.lpushx(key, value);
	}

	public <T extends ProtoEntity> List<T> lrangeProto(final String key, final int start, final int end, final Class<T> protoClass) throws FedisException {
		return lci.lrangeProto(key, start, end, protoClass);
	}

	public List<Integer> lrangeInteger(final String key, final int start, final int end) throws FedisException {
		return lci.lrangeInteger(key, start, end);
	}

	public List<String> lrangeString(final String key, final int start, final int end) throws FedisException {
		return lci.lrangeString(key, start, end);
	}

	public String lsetProto(final String key, final int index, final ProtoEntity value) throws FedisException {
		return lci.lsetProto(key, index, value);
	}

	public String lsetString(final String key, final int index, final String value) throws FedisException {
		return lci.lsetString(key, index, value);
	}

	public String lsetInteger(final String key, final int index, final int value) throws FedisException {
		return lci.lsetInteger(key, index, value);
	}

	public String lset(final String key, final int index, final byte[] value) throws FedisException {
		return lci.lset(key, index, value);
	}

	public String ltrim(final String key, final int start, final int end) throws FedisException {
		return lci.ltrim(key, start, end);
	}
	
	public <T extends ProtoEntity> T rpopProto(final String key, final Class<T> protoClass) throws FedisException {
		return lci.rpopProto(key, protoClass);
	}

	public Integer rpopInteger(final String key) throws FedisException {
		return lci.rpopInteger(key);
	}

	public String rpopString(final String key) throws FedisException {
		return lci.rpopString(key);
	}

	public Long rpushProtos(final String key, final ProtoEntity... values) throws FedisException {
		return lci.rpushProtos(key, values);
	}

	public Long rpushStrings(final String key, final String... values) throws FedisException {
		return lci.rpushStrings(key, values);
	}

	public Long rpushIntegers(final String key, final Integer... values) throws FedisException {
		return lci.rpushIntegers(key, values);
	}

	public Long rpush(final String key, final byte[]... values) throws FedisException {
		return lci.rpush(key, values);
	}

	public Long rpushx(final String key, final ProtoEntity value) throws FedisException {
		return lci.rpushxProto(key, value);
	}

	public Long rpushxString(final String key, final String value) throws FedisException {
		return lci.rpushxString(key, value);
	}

	public Long rpushxInteger(final String key, final int value) throws FedisException {
		return lci.rpushxInteger(key, value);
	}

	public Long rpushx(final String key, final byte[] value) throws FedisException {
		return lci.rpushx(key, value);
	}
}
