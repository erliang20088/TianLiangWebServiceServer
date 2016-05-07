package com.zel.es.utils;

/**
 * 静态变量
 * 
 * @author zel
 * 
 */
public class StaticValue {
	public static String prefix_http = "http://";
	public static String default_encoding = "utf-8";

	public static String separator_tab = "\t";
	public static String separator_next_line = "\n";
	public static String separator_dash_line = "_";
	public static String separator_whitespace = " ";
	public static String separator_none = "";

	public static int ssdb_zset_default_score = 0;

	/**
	 * 得到dt
	 */
	public static String dt_today = DateUtil.getTodateString();
	public static String dt_yesterday = DateUtil.getYesterdayString();
	public static String dt_two_days_ago = DateUtil.getFormatDtFromDaysAgo(2);
	
	
	public static void main(String[] args) {
		System.out.println(dt_two_days_ago);
	}
}
