package com.zel.es.utils;

import java.util.LinkedList; 
import java.util.List;

import com.zel.es.pojos.TestSearchHotWord;
 
/**  
 * 字符串与对象的转换工具类
 *  
 * @author zel
 *  
 */
public class StringToObjectConvertUtil {
	private static MyLogger logger = new MyLogger(
			StringToObjectConvertUtil.class);

	/**
	 * 这里的indexObjClass是指要文本行要转换成的对象，并通过instanceof判断它属于的类，在这里实现文本行与其实际所对应对象的转行
	 * 
	 * @param line
	 * @param indexObjClass
	 * @return
	 */
	private static Object convertToIndexObjectPojo(String line,
			Object indexObjClassOwnToObject) {
		if (StringOperatorUtil.isBlank(line)) {
			return null;
		}
		try {
			if (indexObjClassOwnToObject instanceof TestSearchHotWord) {
				// 为立勇那边准备的,search_hot_keyword
				String[] columnArray = line.split(StaticValue.separator_tab);
				if (columnArray.length != 4) {
					return null;
				}
				try {
					TestSearchHotWord searchHotWord = new TestSearchHotWord();
					searchHotWord.setSearch_keyword(columnArray[0]);
					searchHotWord
							.setSearch_keyword_not_analyzer(columnArray[0]);
					// searchHotWord.setHot(Integer.parseInt(columnArray[1]));
					searchHotWord.setPv(Integer.parseInt(columnArray[1]));
					searchHotWord.setUv(Integer.parseInt(columnArray[2]));
					// searchHotWord
					// .setIndex_num(Integer.parseInt(columnArray[2]));
					searchHotWord.setHost(columnArray[3]);

					return searchHotWord;
				} catch (Exception e) {
					return null;
				}
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	// 转成如TanxPojo,或是 AdCrowdBasePojo等类
	public static List<Object> convertToObjectPojoList(List<String> lineList,
			Object indexObjClassOwnToObject) {
		if (StringOperatorUtil.isBlankCollection(lineList)) {
			return null;
		}
		List<Object> list = new LinkedList<Object>();
		Object obj_temp = null;

		for (String line : lineList) {
			obj_temp = convertToIndexObjectPojo(line, indexObjClassOwnToObject);
			if (obj_temp == null) {
				continue;
			}
			list.add(obj_temp);
		}

		return list;
	}

	public static void main(String[] args) {
		System.out
				.println("ed3dfb8beacf2d56c8abce9ca233d7b4        117     model"
						.split(StaticValue.separator_tab).length);
	}
}
