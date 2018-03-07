package com.unicom.filter;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;

import com.unicom.model.CBSSFile;
import com.unicom.util.ShowUtil;

/**
 * 对CB文件进行filter过滤
 * 应用场景：在对CB文件进行切割之前调用此类方法
 * 
 * @author linzt kendy
 * @time 2018年3月1日 下午2:40:40
 */
public class CBFileFilter {
	
	//读取文件的编码
	private static final Charset CHARSET = Charset.defaultCharset();
	
	//换行
	private static final String NEXT_LINE = System.lineSeparator();

	//CB源文件夹路径  /4Gpositive_nopackage_67_20180201_6001_9900_all.req
//	private static final String CB_RESOURCE_FOLDER_PATH = "C:/Users/linzt8/Desktop/华为/订购关系";
	private static final String CB_RESOURCE_FOLDER_PATH = "C:/Users/linzt8/Desktop/华为/test";
	
	//filter源文件路径（过滤条件记录集）
	private static final String CB_FILTER_FILE_PATH = "C:/Users/linzt8/Desktop/华为/filter.txt";
//	private static final String CB_FILTER_FILE_PATH = "C:/Users/linzt8/Desktop/华为/测试规范.txt";
	
	//过滤条件缓存{PHONENUMBER	： VACSPPRODUCTID},大约有7万个元素
	private static Map<String,String> filterMap = new HashMap<>(70000);
	
	private  static String currentPath = "";
	
	//filter文件的第一行开头
	private static final String FIRST_ROW_CONTENT_START = "PHONENUMBER";
	
	//分隔符
	private static String SPLITOR = "\t"; //CBSS文件的行记录内容是以Tab键隔开的
	
	private static final String SEPRATOR = File.separator;
	
	
	
	
	/**
	 * 构造方法
	 */
	public CBFileFilter() {
		super();
	}
	
	public CBFileFilter(String currentPath) {
		super();
		this.currentPath = currentPath;
	}
	
	public static void main(String... args) throws Exception{
		handleCBFileFilter("");
	}
	
	
	public static void handleCBFileFilter(String outPath) throws Exception{
		if(StringUtils.isBlank(outPath)) {
			outPath = System.getProperty("user.dir");
			
		}
		currentPath = outPath;
		
		CBFileFilter CBFilter = new CBFileFilter();
		//获取过滤条件
		System.out.println("start to read filter.txt ......");
//		List<String> conditions = CBFilter.getFileList(new File(CB_FILTER_FILE_PATH));
		String path = currentPath + SEPRATOR + "filter.txt";
		List<String> conditions = CBFilter.getFileList(new File(path));
		if(CollectionUtils.isEmpty(conditions)) {
			ShowUtil.show("not found filter.txt, the right path is ：" + path);
			return;
		}
		//开始处理，用map结构来存储以提升检索性能，{PHONENUMBER#VACSPPRODUCTID	： null}
		filterMap.clear();
		conditions.stream()//.skip(1)
			.filter(record -> !(record.toUpperCase().startsWith(FIRST_ROW_CONTENT_START)))
			.forEach(condition -> {
				String[] rowContentArr = condition.split(SPLITOR);
				String phoneNumber = rowContentArr[0];
				String vacSpProductId =  rowContentArr[1];
				filterMap.put(phoneNumber+"#"+vacSpProductId, "");
			});
		//额外判断是否遗漏或者重复
		int conditionSize = conditions.size();
		int filterMapSize = filterMap.size();
		String compareResult = conditionSize - filterMapSize == 0 ? "yes" : "no";
		System.out.println(String.format("filter numbers ：%d  cache：%d  equals：%s", conditionSize, filterMapSize, compareResult));
		if(conditionSize != filterMapSize) {
			ShowUtil.show(String.format("error: filter record lines is %d, cache lines is %d, these two number should be equal！",conditionSize, filterMapSize));
		}
		//处理CB
		List<File> cbssFileList = CBFilter.getCBSSFileList();
		if(CollectionUtils.isEmpty(cbssFileList)) {
//			ShowUtil.show("找不到CB文件，当前路径：" + CB_RESOURCE_FOLDER_PATH);
			ShowUtil.show("not found CB file, the current path is ：" + currentPath);
			return;
		}
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < cbssFileList.size(); i++) {
			
			File cbFile = cbssFileList.get(i);
			System.out.println(String.format("handling the %d file，CB file name is ：%s", i, cbFile.getName()));
			List<String> cbAllContents = CBFilter.getFileList(cbFile);

			// 缓存CB文件内容
			String firstRow = cbAllContents.get(0);
			List<String> dataList = cbAllContents.parallelStream() //并行处理更快
												 .skip(1)//跳过第一行
												 .filter(record -> { //过滤
													 	String key = getPhoneNumberWithProductId(record);
													 	return filterMap.containsKey(key) ;
												  })
												 .collect(Collectors.toList());
			
			// 生新新CB文件 
			CBSSFile newCBFile = new CBSSFile(cbFile.getAbsolutePath(), firstRow, dataList);
			new Thread(
					new CBSSFileCreator(newCBFile)
			).start();
			
		}
		long end = System.currentTimeMillis();
		print("handle filter takes time ："+(end - start) + "ms");
	}
	
	/**
	 * 获取手机号与产品ID组合字符串
	 * 
	 * @param cbRowContent
	 * @return
	 */
	private static String getPhoneNumberWithProductId(String cbRowContent) {
		String[] cbRowContentArr = cbRowContent.split(SPLITOR);
		String phoneNumber = cbRowContentArr[8];
		String vacSpProductId =  cbRowContentArr[11];
		String key = phoneNumber + "#" + vacSpProductId;
		return key;
	}
	
	
	private List<String> getFileList(File file){
		List<String> fileContents = null;
		try {
			fileContents = FileUtils.readLines(file, CHARSET);
		}catch(Exception e){
			fileContents = Collections.EMPTY_LIST;
			e.printStackTrace();
		}
		return fileContents;
	}
	
	/**
	 * 获取初始文件列表
	 */
	@SuppressWarnings("unchecked")
	public  List<File> getCBSSFileList(){
		List<File> listFiles = null;
		try {
//			File file = new File(CB_RESOURCE_FOLDER_PATH);
			File file = new File(currentPath);
			listFiles = (List<File>) FileUtils.listFiles(file, FileFilterUtils.prefixFileFilter("4Gpositive_nopackage"), null);
			for (File f : listFiles)  System.out.println("getCBFile====="+f.getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
        
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
	 * 打印
	 * @param obj
	 */
	public static void print(Object obj) {
		System.out.println(obj);
	}
	
	
	
	
	
	/**
	 * 静态内部类：专门生成CBSS文件的线程
	 * 
	 * @author linzt
	 * @time 2018年3月1日 下午3:03:09
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
				FileUtils.write(file, data, Charset.defaultCharset());
				long end = System.currentTimeMillis();
				System.out.println(" create CB files , takes time :"+(end-start));
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
