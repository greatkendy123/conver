package com.unicom.util;

import java.util.Timer;
import java.util.TimerTask;

import javax.swing.JDialog;
import javax.swing.JOptionPane;

/**
 * 自定义弹框显示
 * @author 林泽涛
 * @time 2017年12月24日 下午2:23:18
 */
public class ShowUtil {

	/**
	 * 带有自动关闭功能的提示框
	 * 
	 * @time 2017年11月18日
	 * @param message
	 * @param type
	 */
	 public static void show(String message,int type){
		 if(type == -1)
			 JOptionPane.showMessageDialog(null, message, "错误",JOptionPane.ERROR_MESSAGE); 
		 else
			 JOptionPane.showMessageDialog(null, message, "提示",JOptionPane.PLAIN_MESSAGE);  
	  }
	 
	 /**
	  * 弹框提示
	  * @param message
	  */
	 public static void show(String message){
		 JOptionPane.showMessageDialog(null, message, "提示",JOptionPane.PLAIN_MESSAGE); 
	  }
	 
	 
	  
}
