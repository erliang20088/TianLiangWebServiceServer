package com.zel.es.utils;

import io.searchbox.core.Index;

import java.util.LinkedList;
import java.util.List;

/**
 * object to index object convert
 * 
 * @author zel
 * 
 */
public class ObjectToIndexOperatorUtil {

	public static List<Index> convertToIndexList(List pojoList) {
		if (StringOperatorUtil.isBlankCollection(pojoList)) {
			return null;
		}
		List<Index> indexList = new LinkedList<Index>();
		Index index = null;
		for (Object obj : pojoList) {
			index = new Index.Builder(obj).build();
			indexList.add(index);
		}
		return indexList;
	}
}
