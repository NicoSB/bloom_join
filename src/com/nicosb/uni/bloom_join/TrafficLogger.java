package com.nicosb.uni.bloom_join;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.instrument.Instrumentation;

public class TrafficLogger {
	private static Instrumentation instrumentation;
	public static int loggedTraffic = 0;
	public static Object readObject(ObjectInputStream input){
		try {
			Object o = input.readObject();
			loggedTraffic += instrumentation.getObjectSize(o);
			return 0;
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
