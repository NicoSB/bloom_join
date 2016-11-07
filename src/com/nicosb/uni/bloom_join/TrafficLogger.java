package com.nicosb.uni.bloom_join;

import java.io.IOException;
import java.io.ObjectInputStream;

public class TrafficLogger {
	public static <T> T readObject(ObjectInputStream input){
		try {
			@SuppressWarnings("unchecked")
			T o = (T)input.readObject();
			CustomLog.sizeLog(o);
			return o;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
