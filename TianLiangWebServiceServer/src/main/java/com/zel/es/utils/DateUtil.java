package com.zel.es.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	public static int dateStringLength = "yyyy-MM-dd HH:mm:ss".length();

	public SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public SimpleDateFormat yyyyMMddHHmm = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm");

	public static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");

	public static SimpleDateFormat yyyyMMddHH_NOT_ = new SimpleDateFormat(
			"yyyyMMdd");

	public static long DATEMM = 24 * 60 * 60;

	public static long getTimeNumberToday() {
		Date date = new Date();
		String str = yyyyMMdd.format(date);
		try {
			date = yyyyMMdd.parse(str);
			return date.getTime() / 1000L;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public static String getTodateStringWithSplit() {
		Date date = new Date(System.currentTimeMillis());
		String str = yyyyMMdd.format(date);
		return str;
	}

	public static String getTodateString() {
		Date date = new Date(System.currentTimeMillis());
		String str = yyyyMMddHH_NOT_.format(date);
		return str;
	}

	public static String getYesterdayString() {
		Date date = new Date(System.currentTimeMillis() - DATEMM * 1000L);
		String str = yyyyMMddHH_NOT_.format(date);
		return str;
	}

	public static String getFormatDtFromDaysAgo(int day_num) {
		Date date = new Date(System.currentTimeMillis() - day_num * DATEMM
				* 1000L);
		String str = yyyyMMddHH_NOT_.format(date);
		return str;
	}

	public static int getIntDate(String dateString) {
		System.out.println("date string:" + dateString);
		String[] str = dateString.split("-");
		int all = 0;
		try {
			str[1] = ((Integer.parseInt(str[1])) < 10 ? ("0" + Integer
					.parseInt(str[1])) : str[1]);
			str[2] = ((Integer.parseInt(str[2])) < 10 ? ("0" + Integer
					.parseInt(str[2])) : str[2]);
			all = Integer.parseInt(str[0] + str[1] + str[2]);
		} catch (Exception e) {
			System.out.println("日期转化时出现错误!");
			all = 99999999;
		}
		return all;
	}

	public static Date dd = null;

	public static long getTimeLong(int years) {
		dd = new Date();
		dd.setYear(dd.getYear() + years);
		// Timestamp now2 = new Timestamp(dd.getTime());

		return dd.getTime();
	}

	public static long getLongByDate() {
		return new Date().getTime();
	}

	public Date getDateByString(String date_str) throws Exception {
		date_str = date_str.trim();
		if (date_str.length() == dateStringLength && date_str.startsWith("20")) {
			return yyyyMMddHHmmss.parse(date_str);
		} else {
			throw new Exception();
		}
	}

	public String getStringByDate(Date date) throws Exception {
		return yyyyMMddHHmmss.format(date);
	}

	String temp_time_string = null;

	public String getPhpLongTime() {
		temp_time_string = "" + new Date().getTime() + 30000;
		// System.out.println(temp_time_string);
		return temp_time_string.substring(0, temp_time_string.length() - 3);
	}

	public static long getTimeByLevel(int taskLevel) {
		switch (taskLevel) {
		case 1:
			taskLevel = 10;
			break;
		case 2:
			taskLevel = 30;
			break;
		case 3:
			taskLevel = 120;
			break;
		case 4:
			taskLevel = 240;
			break;
		case 5:
			taskLevel = 1440;
			break;
		default:
			taskLevel = 1440;
			break;
		}
		return taskLevel * 60 * 1000;
	}

	String temp_time = null;

	public Date getDateByNoneStruture4Sina(String publishTimeString)
			throws ParseException {
		if (publishTimeString.contains("\"")) {
			publishTimeString = publishTimeString.trim().substring(1,
					publishTimeString.trim().length() - 1);
		}
		if (publishTimeString.contains("月")) {
			String temp_date = "" + (1900 + new Date().getYear());
			temp_time = publishTimeString.substring(0,
					publishTimeString.indexOf("月"));
			if (temp_time.length() == 1) {
				temp_time = "0" + temp_time;
			}
			temp_date = temp_date + "-" + temp_time;
			temp_time = publishTimeString.substring(
					publishTimeString.indexOf("月") + 1,
					publishTimeString.indexOf("日"));
			if (temp_time.length() == 1) {
				temp_time = "0" + temp_time;
			}
			temp_date = temp_date + "-" + temp_time;
			temp_time = publishTimeString.substring(
					publishTimeString.indexOf(" ") + 1,
					publishTimeString.length());
			temp_date = temp_date + " " + temp_time + ":00";
			// System.out.println("时间---"+temp_time);
			// System.out.println("temp_date----"+temp_date);
			return yyyyMMddHHmmss.parse(temp_date);
		} else if (publishTimeString.contains("今天")) {
			publishTimeString = publishTimeString.substring(
					publishTimeString.indexOf(" "), publishTimeString.length())
					.trim();
			publishTimeString += ":00";
			temp_time = yyyyMMddHHmmss.format(new Date());
			temp_time = temp_time.substring(0, temp_time.indexOf(" "));
			publishTimeString = temp_time + " " + publishTimeString;

			return yyyyMMddHHmmss.parse(publishTimeString);
		} else if (publishTimeString.contains("分钟")) {
			temp_time = publishTimeString.substring(0,
					publishTimeString.indexOf('分'));
			// 通过化成ms后的相减操作来完成减多少minutes的操作
			return new Date(getLongByDate() - Integer.parseInt(temp_time) * 60
					* 1000);
		} else if (publishTimeString.contains("-")) {
			String temp_all = null;
			if (publishTimeString.length() == "yyyy-mm-dd hh:mm".length()) {
				publishTimeString = publishTimeString + ":00";
				return yyyyMMddHHmmss.parse(publishTimeString);
			} else {
				temp_time = publishTimeString.split(" ")[0];
				String[] temp_array = temp_time.split("-");
				for (String temp : temp_array) {
					if (temp.length() == 4) {
						temp_all = temp;
					} else if (temp.length() == 1) {
						temp_all += ("-0" + temp);
					} else {
						temp_all += ("-" + temp);
					}
				}
				temp_all += " " + publishTimeString.split(" ")[1] + ":00";
			}
			return yyyyMMddHHmmss.parse(temp_all);
		} else if (publishTimeString.contains("秒")) {
			return new Date();
		} else {
			return new Date();
		}
	}

	public static String dateLongToMMHHssString(Long time) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		return format.format(cal.getTime());
	}

	public String formatLongToMMHHssString(String time) {
		try {
			return yyyyMMddHHmmss.format(Long.parseLong(time));
		} catch (Exception e) {
			return "";
		}
	}

	public String formatDtString(long time) {
		try {
			return yyyyMMddHH_NOT_.format(time);
		} catch (Exception e) {
			return "";
		}
	}

	// 不可大数据量使用
	public static String getServeralDaysAgo(int daysNumber) {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1 * daysNumber);
		return format.format(cal.getTime());
	}

	// 不可大数据量使用
	public static long getMinMsOfDate(int year, int month, int dayNumber) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month, dayNumber, 0, 0, 0);
		long ts = calendar.getTimeInMillis();
		String ts_str = ("" + ts).substring(0, ("" + ts).length() - 3) + "000";
		return Long.parseLong(ts_str);
	}

	public static long getMaxMsOfDate(int year, int month, int dayNumber) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month, dayNumber, 23, 59, 59);
		long ts = calendar.getTimeInMillis();
		String ts_str = ("" + ts).substring(0, ("" + ts).length() - 3) + "999";
		return Long.parseLong(ts_str);
	}

	public static void main(String[] args) throws Exception {
		System.out.println(DateUtil.getFormatDtFromDaysAgo(2));
	}
}
