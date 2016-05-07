package com.zel.es.utils;

import java.security.MessageDigest;

/**
 * md5加密工具类,静态方法是线程安全的，非静态方法是非线程安全的
 * 
 * @author zel
 * 
 */
public class MD5OperatorUtil {

	private MessageDigest md5 = null;

	public MD5OperatorUtil() {
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			System.out
					.println("MD5OperatorUtil==md5 encode occur error,please check!");
			e.printStackTrace();
		}
	}

	// MD5加码。32位
	public String MD5(String inStr) {
		char[] charArray = inStr.toCharArray();
		byte[] byteArray = new byte[charArray.length];

		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];

		byte[] md5Bytes = md5.digest(byteArray);

		StringBuilder hexValue = new StringBuilder();

		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}
		return hexValue.toString().toUpperCase();
	}

	// MD5加码。32位
	public String MD5_little(String inStr) {
		char[] charArray = inStr.toCharArray();
		byte[] byteArray = new byte[charArray.length];

		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];

		byte[] md5Bytes = md5.digest(byteArray);

		StringBuilder hexValue = new StringBuilder();

		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}
		return hexValue.toString();
	}

	// 可逆的加密算法
	public static String KL(String inStr) {
		// String s = new String(inStr);
		char[] a = inStr.toCharArray();
		for (int i = 0; i < a.length; i++) {
			a[i] = (char) (a[i] ^ 't');
		}
		String s = new String(a);
		return s;
	}

	// 加密后解密
	public static String JM(String inStr) {
		char[] a = inStr.toCharArray();
		for (int i = 0; i < a.length; i++) {
			a[i] = (char) (a[i] ^ 't');
		}
		String k = new String(a);
		return k;
	}

	// 测试主函数
	public static void main(String args[]) {
		MD5OperatorUtil md5OperatorUtil = new MD5OperatorUtil();
		String inStr = "869611021658632";
		System.out.println(md5OperatorUtil.MD5_little(inStr));
		System.out.println(md5OperatorUtil.MD5_little(inStr).length());
	}
}
