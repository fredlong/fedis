package com.fedis.router;

import com.fedis.util.FedisUtils;

public class RedisNode {
	
	String roleName = FedisUtils.EMPTY_STRING;
	int id = -1;
	String host = FedisUtils.EMPTY_STRING;
	int port = -1;
	int weight = 0;
	
	
	public String getRoleName() {
		return roleName;
	}
	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}

	
	
}
