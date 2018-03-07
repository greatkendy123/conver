package com.unicom.conver;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;

import com.unicom.model.CBSSFile;
import com.unicom.util.ShowUtil;

/**
 * CBSS文件切割，重新生成话单文件
 * 
 * 有7个CBSS大文件，每个CBSS文件最高150万条记录
 * 切割的文件个数大约 7 * 1500000 / 10000 = 1050 个CB小文件
 * 
 * @author 林泽涛
 * @time 2017年12月23日 下午10:56:39
 */
public class CBFileSplit2 {

	//初始化CBSS文件队列容量（目前是1050个）
	private static final Integer MAX_FILE_QUEUE_SIZE = 2000;
	
	//生成CBSS文件的线程数
	private static final Integer CREATE_FILE_THREAD_SIZE = 2;
	
	//每个线程处理的CBSS文件个数
	private static final Integer HANDLE_FILE_SIZE = 1;
	
	//待重新生成的CBSS文件队列
	private static BlockingQueue<CBSSFile> creat_file_queue =
			new ArrayBlockingQueue<>(MAX_FILE_QUEUE_SIZE);

	//创建新CBSS文件的线程池
	private static ExecutorService create_poor =
			Executors.newFixedThreadPool(CREATE_FILE_THREAD_SIZE);
	
	private static String currentPath = "";
	
	private static final String SEPRATOR = File.separator;
	
	private static String outPath = "";
	
	private static String SPACE = "\t"; //CBSS文件的行记录内容是以Tab键隔开的
	
	
	private static AtomicInteger atomicInteger2 = new AtomicInteger(1);
	
	private static AtomicInteger atomicInteger = new AtomicInteger(1);
	
	private static final Integer FILE_LINES = 9999; 
	
	private static final String REQ = ".req";
	
	private static Integer createFileSize = 0;
	
	private static Integer bigFileSzie = 0;
	
	
	private static String fileName = "";
	
	private static String firstRow = "";
	/**
	 * 构造方法
	 */
	public CBFileSplit2(String currentPath) {
		super();
		this.currentPath = currentPath;
	}

	/**
	 * 打印信息
	 * @time 2017年12月24日
	 */
	public static void printInfo() {
		System.out.println();
		System.out.println("==================================================处理CBSS文件信息=====================");
		System.out.println("current path : "+System.getProperty("user.dir"));
		System.out.println("the threads number of creating CB file : "+CREATE_FILE_THREAD_SIZE);
		System.out.println("handle CB file size of each thread : "+HANDLE_FILE_SIZE);
		System.out.println("find CB files number : "+bigFileSzie);
		System.out.println("=====================================================================================");
		System.out.println();
	}
	
	/**
	 * 测试入口
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		String outPath = "C:/Users/linzt8/Desktop/华为/test";
		handleCBSS(outPath);
	}
	
	/**
	 * 处理CBSS文件调用入口
	 * 需要较大的内存来执行此方法（会在内存中处理两千多万条CB数据）
	 * @time 2017年12月24日
	 * @param outPath
	 * @throws Exception
	 */
	public static void handleCBSS(String outPath) throws Exception{
		if(StringUtils.isBlank(outPath)) {
			outPath = System.getProperty("user.dir");
		}
		currentPath = outPath;
		CBFileSplit2 cBFileSplit = new CBFileSplit2(outPath);

		Collection<File> listFiles = cBFileSplit.getCBSSFileList();
		if(listFiles.isEmpty()) {
			ShowUtil.show(currentPath+", this path contains no CB file!",-1);  
			return;
		}
		
		printInfo();
		
		bigFileSzie = listFiles.size();
		
		if( bigFileSzie > 0) {
			File file = ((List<File>)listFiles).get(0);
			List<String> contentAllList = FileUtils.readLines(file, Charset.defaultCharset());
			
			fileName = file.getName();
			firstRow = contentAllList.get(0);
		}
		
		
		List<String> cacheList = new ArrayList<>();//缓存所有数据，预计只有十万条以内
		for(File file  : listFiles) {
			
			List<String> contentAllList = FileUtils.readLines(file, Charset.defaultCharset());
			
			List<String> contentDataList = contentAllList.subList(1, contentAllList.size());
			System.out.println("===============subDataList record: "+contentAllList.size() + "===" +contentDataList.size());
			cacheList.addAll(contentDataList);
			
		}
		
		int allSize = cacheList.size();
		int size = cacheList.size();//allSize-1
		int count =  size / FILE_LINES;
		if( size % FILE_LINES != 0 ) {
			count += 1;
		}
		createFileSize += count;
		//System.out.println("总"+size+"条Data记录");
		int from = 0, to = 0;
		for(int i=0; i<count; i++) {
			from = to;
			to =  from + FILE_LINES > allSize ? size :  from + FILE_LINES;
			
			//System.out.println("起："+from+" 止："+to);
			
			//构造CB文件所需元素
			List<String> subDataList = cacheList.subList(from, to);
			
			subDataList = handleFirstColumn(subDataList);
			String _fileName = getFileNameString(fileName,i+1);
			String _firstRow = getFirstRowString(firstRow.trim(),subDataList.size());
			CBSSFile cbssFile = new CBSSFile(_fileName,_firstRow,subDataList);
			
			//添加到生产文件队列
			add2CreateQueue(cbssFile);
		}
		 
		cBFileSplit.quotaTask();
		
		System.out.println("CBSS main thread finishes ！");
	}
	
	/**
	 * 华为要求修改CB文件的第一列最后四个数字为行的索引
	 * 
	 * @param subDataList
	 * @return
	 */
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
	
	/**
	 * 获取格式化后的字符串（格式化数字为指定格式的字符串）
	 * 此方法估计会调用一千多 万次
	 * @param value
	 * @param index
	 * @return
	 */
	public static String getFormartIndex(String value,int index) {
		String subVal = value.substring(0, 14);
		String formatIndex = String.format("%04d" , index);
		return subVal+formatIndex;
	}
	
	/**
	 * 为线程池中的线程分配任务
	 * @time 2017年12月23日
	 */
	public void quotaTask() {
		for(int i=0;i<createFileSize;i++) {
			try {
				//往线程池添加任务： 生成新Vac文件
				create_poor.submit(new CBSSFileCreator(creat_file_queue.take()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		create_poor.shutdown();
		System.out.println("quotar the task to the thread which is in thread poor ");
	}
	
	/**
	 * 获取格式化后切割文件名称
	 * @time 2017年12月24日
	 * @param fileName
	 * @param index
	 * @return
	 */
	public static String getFileNameString(String fileName, int index) {
		//"4Gpositive_nopackage_67_20171212_8001_9900_all.req";
		
		String _prefix = fileName.substring(0,fileName.lastIndexOf("_all"));
		//System.out.println(_prefix);
		String prefix = fileName.substring(0,_prefix.lastIndexOf("_"));
		prefix = fileName.substring(0,prefix.lastIndexOf("_")+1);
		//System.out.println(prefix);
		String formatIndex = String.format("%04d" , index);
		
		String suffix = fileName.substring(fileName.length()-13); //从后向前提取 _9900_all.req
		String _fileName = prefix + formatIndex + suffix;

		fileName =  currentPath+SEPRATOR+
					"result"+SEPRATOR+
					"cbss"+SEPRATOR+
					_fileName;
		return fileName;
	}
	
	/**
	 * 获取每个CB文件的第一行
	 * @time 2017年12月24日
	 * @param firstRow
	 * @return
	 */
	public static String getFirstRowString(String _firstRow,int index) {
		//8001000002017121217321209920171212173212201712122336220000990000
		//0002000002017120101010290219700101000000209901010000000000000001
		String indexString = String.format("%07d", index);
		_firstRow = _firstRow.substring(0, _firstRow.length()-7);
		return _firstRow + indexString;
	}
	
	/**
	 * 添加到生产文件队列
	 * @time 2017年12月24日
	 * @param cbssfile
	 */
	public static void add2CreateQueue(CBSSFile cbssfile) {
		try {
			creat_file_queue.put(cbssfile);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 获取初始文件列表
	 */
	public  Collection<File> getCBSSFileList(){
		File file = new File(currentPath);
		Collection<File> listFiles = FileUtils.listFiles(file, FileFilterUtils.prefixFileFilter("4Gpositive_nopackage"), null);
		//for (File f : listFiles)  System.out.println("getFile====="+f.getName());
        
		return listFiles == null ? Collections.EMPTY_LIST : listFiles ;
	}
	
	/**
	 * 获取一个CBSS文件的所有数据（缓存）
	 * @time 2017年12月23日
	 * @param list
	 * @return
	 */
	public static String getCBSSDataString(CBSSFile cbssFile) {
		List<String> dataList = cbssFile.getDataList();
		if(dataList==null) return "";
		StringBuffer sb = new StringBuffer();
		sb.append(cbssFile.getFirstRow()).append(System.lineSeparator());
		dataList.forEach(recordString -> {
			sb.append(recordString)
			  .append(System.lineSeparator());
		});
		return sb.toString();
	}
	
	/**
	 * 静态内部类：专门生成CBSS文件的线程
	 * @author 林泽涛
	 * @time 2017年12月23日 下午5:20:08
	 */
	static class CBSSFileCreator implements Runnable{
		
		private CBSSFile cbssFile ;
		
		public CBSSFileCreator(CBSSFile cbssFile) {
			super();
			this.cbssFile = cbssFile;
		}

		@Override
		public void run(){
			try {
				//生成Vac文件到指定目录下
				long start = System.currentTimeMillis();
				File file = new File(cbssFile.getFileName());
				String data = getCBSSDataString(cbssFile);
				FileUtils.write(file, data,Charset.defaultCharset());
				long end = System.currentTimeMillis();
				System.out.println("creat CB file, takes time ："+(end-start)+", 第"+atomicInteger2.getAndIncrement());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public CBSSFile getCbssFile() {
			return cbssFile;
		}

		public void setCbssFile(CBSSFile cbssFile) {
			this.cbssFile = cbssFile;
		}
	}
}
