package com.fedis.router;

import java.util.Set;

import com.fedis.util.FedisException;



public interface ReRoutable {

	public Set<String> getAllDataKey(String[] keyPatterns) throws FedisException;

	public void moveData(String dataKey, ReRoutable destNode) throws FedisException;
	
	public void deleteData(String dataKey) throws FedisException;
	
	public String toString();
}

