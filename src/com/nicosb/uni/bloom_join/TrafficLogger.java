package com.nicosb.uni.bloom_join;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.instrument.Instrumentation;

public class TrafficLogger {
	private static Instrumentation instrumentation;
	public static int loggedTraffic = 0;
	public static <T> T readObject(ObjectInputStream input){
		try {
			T o = (T)input.readObject();
			CustomLog.sizeLog(o);
			//loggedTraffic += instrumentation.getObjectSize(o);
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
