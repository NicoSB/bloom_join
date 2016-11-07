package com.nicosb.uni.bloom_join;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

public class CustomLog {
	public static boolean printToConsole = true;
	public static boolean printToFile = false;
	public static String logFile = "log.txt";
	public static String cache_size_file = "csf";
	
	public static <T> void println(T str){
		if(printToConsole){
			System.out.println(str);
		}
		if(printToFile){
			try {
				PrintWriter out = new PrintWriter(new FileWriter(logFile, true));
				out.write(str + "\n");
				out.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
	
	public static <T> void print(T str){
		if(printToConsole){
			System.out.print(str);
		}
		if(printToFile){
			try {
				PrintWriter out = new PrintWriter(new FileWriter(logFile, true));
				out.write(String.valueOf(str));
				out.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void sizeLog(Object o){
		try {
			FileOutputStream fis = new FileOutputStream(cache_size_file, true);
			ObjectOutputStream oos = new ObjectOutputStream(fis);
			oos.writeObject(o);
			oos.close();
			fis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public static long getTraffic(boolean delete){
		File f = new File(cache_size_file);
		long size = f.exists() ? f.length() : 0;
		if(size > 0 && delete){
			f.delete();
		}
		return size;
	}
}
