package com.nicosb.uni.bloom_join;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class CustomLog {
	public static boolean printToConsole = true;
	public static boolean printToFile = false;
	public static String logFile = "log.txt";
	
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

}
