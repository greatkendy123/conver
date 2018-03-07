package com.unicom.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class Test {

	/**
	 * 测试
	 * @time 2017年12月24日
	 * @param args
	 */
	public static void main(String[] args) {

	}
	
	public static void readFileTest(String filePath) throws Exception{
		File file = new File(filePath);
		List<String> readLines = FileUtils.readLines(file, Charset.defaultCharset());
	}
	
	
	
	
	
	public static void testLog() throws IOException { 
		Logger log = Logger.getLogger("lavasoft"); 
		log.setLevel(Level.INFO); 
		Logger log1 = Logger.getLogger("lavasoft"); 
		System.out.println(log == log1);     //true 
		Logger log2 = Logger.getLogger("lavasoft.blog"); 
		// log2.setLevel(Level.WARNING);
		
		ConsoleHandler consoleHandler = new ConsoleHandler(); 
		consoleHandler.setLevel(Level.ALL); 
		log.addHandler(consoleHandler); 
		FileHandler fileHandler = new FileHandler("C:/testlog%g.log"); 
		fileHandler.setLevel(Level.INFO); 
		fileHandler.setFormatter(new MyLogHander()); 
		log.addHandler(fileHandler); 
		
		log.info("aaa"); 
		log2.info("bbb"); 
		log2.fine("fine"); 
	} 

	static class MyLogHander extends Formatter { 
	        @Override 
	        public String format(LogRecord record) { 
	                return record.getLevel() + ":" + record.getMessage()+"\n"; 
	        } 
	}
	
	public static List<String> handleFirstColumn(List<String> subDataList) {
		String indexVal = "";
		String rowString = "";
		String suffixString="";
		String formatIndex ="";
		String prefixString="";
		List<String> newList = new LinkedList<>();
		for(int i=0;i<subDataList.size();i++) {
			rowString = subDataList.get(i);
			prefixString = rowString.substring(0, 14);
			indexVal = rowString.substring(14,18);
			suffixString = rowString.substring(18);
			formatIndex = String.format("%04d" , i+1);
			String newString = prefixString + formatIndex + suffixString;
			newList.add(newString);
		}
		subDataList = newList;
		return subDataList;
	}
	
	public static String getFormartIndex(String value,int index) {
		String subVal = value.substring(0, 14);
		String formatIndex = String.format("%04d" , index);
		return subVal+formatIndex;
	}
	
	public static void getFileNameString() {
		String fileName = "4Gpositive_nopackage_67_20171212_8001_9900_all.req";
		System.out.println(fileName);
		int index = 21;
		String _prefix = fileName.substring(0,fileName.lastIndexOf("_all"));
		System.out.println(_prefix);
		String prefix = fileName.substring(0,_prefix.lastIndexOf("_"));
		prefix = fileName.substring(0,prefix.lastIndexOf("_")+1);
		System.out.println(prefix);
		String formatIndex = String.format("%04d" , index);
		
		String suffix = fileName.substring(fileName.length()-13); //从后向前第i位提取
		
		String _fileName = prefix + formatIndex + suffix;
		
		System.out.println(_fileName);
	}
	
	public static void getFirstRow() {
		String row = "0002000002017120121001";
		int index = 64;
		String indexString = String.format("%04d", index);
		row = row.substring(0, row.length()-4);
		System.out.println(row+indexString);
	}
	
	/**
	 * 模拟获取不同操作系统下的当前路径
	 * @time 2017年12月24日
	 * @return
	 */
	public static String  getCurrentPath() {
		
		return "";
	}
	
	/**
	 * 模拟产生1000个VAC文件
	 * @time 2017年12月24日
	 * @throws Exception
	 */
	public static void createVacFiles() throws Exception {
    	String _path = "C:/Users/kendy/Desktop/HuaWei/";
    	String fileName = "SubscribeInfo000901152017120504100183760";
    	String root = "C:/Users/kendy/Desktop/HuaWei/test/";
    	System.out.println("start...");
    	for(int i=1;i<1000;i++)
    		FileUtils.copyFile(new File(_path+fileName+".req"), new File(root+fileName+i+".req"));
    	
    	System.out.println("finishes!");
	}

}
