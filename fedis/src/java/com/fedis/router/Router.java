package com.fedis.router;

import java.util.List;

import com.fedis.util.Action;
import com.fedis.util.Combo3;
import com.fedis.util.FedisException;

public interface Router<T extends ReRoutable> {

	public void initNode(T node, int weight) throws FedisException;

	public void addNode(T node, int weight) throws FedisException;

	public void removeNode(T node, int weight) throws FedisException;

	public T get(String key) throws FedisException;

	public List<Combo3<String, T, T>> reHashAsync(boolean isMoveData);

	public void reHashSync(boolean isMoveData, String[] keyPatterns, Action<Combo3<Integer, Integer, String>> progressCallback, Action<List<Combo3<String, T, T>>> finallyCallback);

}