package com.fedis.router;

import com.fedis.util.FedisException;


public interface HashHelper {
	
	public int hash(String keyValue) throws FedisException;
	
}
