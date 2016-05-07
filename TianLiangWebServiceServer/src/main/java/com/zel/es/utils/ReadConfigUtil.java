package com.zel.es.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 配置文件读取工具类
 * 
 * @author zel
 * 
 */
public class ReadConfigUtil {
	public InputStream in = null;
	public BufferedReader br = null;
	private Properties config = null;

	// 此时的configFilePath若是非普通文件，即properties文件的话，要另行处理
	public ReadConfigUtil(String configFilePath, boolean isConfig) {
		in = ReadConfigUtil.class.getClassLoader().getResourceAsStream(
				configFilePath);
		try {
			if (isConfig) {
				config = new Properties();
				config.load(in);
				in.close();
			} else {
				br = new BufferedReader(new InputStreamReader(in));
			}
		} catch (IOException e) {
			System.out.println("加载配置文件时，出现问题!");
		}
	}

	public String getValue(String key) {
		try {
			String value = config.getProperty(key);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("ConfigInfoError" + e.toString());
			return null;
		}
	}

	public String getTextLines() {
		StringBuilder sb = new StringBuilder();
		String temp = null;
		try {
			while ((temp = br.readLine()) != null) {
				if (temp.trim().length() > 0 && (!temp.trim().startsWith("#"))) {
					sb.append(temp);
					sb.append("\n");
				}
			}
			br.close();
			in.close();
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("读取配置文件时，出现问题!");
		}
		return sb.toString();
	}

	public static void main(String args[]) {
		// ReadConfigUtil readConfig=new ReadConfigUtil("slaves");
		// System.out.println(readConfig.getTextLines());
	}
}
