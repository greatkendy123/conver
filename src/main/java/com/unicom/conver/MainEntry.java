package com.unicom.conver;

import org.apache.commons.lang3.StringUtils;

import com.unicom.filter.CBFileFilter;
import com.unicom.util.ShowUtil;

/**
 * VAC内容格式化 与 CBSS文件切割
 * 
 * VAC文件内容最高需处理 10000 * 1500 = 1500万条记录
 * 处理CB文件大约需生成 7 * 1500000 /10000 = 1050 个CB文件，每个文件1万条记录
 * @author 林泽涛
 * @time 2017年12月24日 下午1:34:37
 */
public class MainEntry {
	
	/**
	 * 主程序入口
	 * @time 2017年12月24日
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args)  throws Exception{
    	if( args.length == 0 ) {
    		ShowUtil.show("参数不能为空，详见调用说明", -1);
    		return;
    	}
    	String callType = args[0];
    	if(StringUtils.equals(callType, "vac")) {
    		VacConvert.handleVAC("");
    		
    	}else if(StringUtils.equals(callType, "cbss")){
    		CBFileSplit.handleCBSS("");
    		
    	}else if(StringUtils.equals(callType, "filter")){
    		CBFileFilter.handleCBFileFilter("");
    		
    	}else {
    		ShowUtil.show("参数无效！", -1);
    	}
    }
}


