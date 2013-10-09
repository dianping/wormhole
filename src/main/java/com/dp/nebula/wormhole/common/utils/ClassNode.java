package com.dp.nebula.wormhole.common.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ClassNode {
	private List<ClassMember> members;

	private String name;

	public static ClassNode newInstance() {
		return new ClassNode();
	}

	public static ClassMember createMember(String name, Map<String, String> attrs) {
		return new ClassMember(name, attrs);
	}

	private ClassNode() {
		members = new ArrayList<ClassMember>();
	}

	public void addMember(ClassMember cm) {
		this.members.add(cm);
	}

	public List<ClassMember> getAllMembers() {
		return this.members;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Class name: %s\n", this.name));
		for (ClassMember cm: this.members) {
			sb.append(cm.toString());
		}
		return sb.toString();
	}
}

class ClassMember {
	private String name;

	private Map<String, String> attris = new HashMap<String, String>();
	
	ClassMember(String name, Map<String, String> attrs) {
		this.name = name;
		this.attris = attrs;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void addAttr(String key, String value) {
		this.attris.put(key, value);
	}

	public String getAttr(String key) {
		return this.attris.get(key);
	}

	public Set<String> getAllKeys() {
		return this.attris.keySet();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("name: %s\n", this.name));
		for (String key: attris.keySet()) {
			sb.append(String.format("key: %s, value: %s\n", key, attris.get(key)));
		}
		return sb.toString();
	}
	
}
