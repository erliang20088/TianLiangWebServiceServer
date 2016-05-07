package com.zel.es.manager.other;

import java.util.LinkedList;
import java.util.List;

import com.zel.entity.TermUnit;
import com.zel.es.pojos.nlp.AnalyzerResultPojo;

/**
 * 将term unit 转化为 analyzer result pojo对象，减少依赖
 * 
 * @author zel
 * 
 */
public class AnalyzerResultConvertManager {
	public static List<AnalyzerResultPojo> convertTermUnitToPojoWithPos(
			List<TermUnit> termUnitList) {
		if (termUnitList != null && (!termUnitList.isEmpty())) {
			// List<String> linkedList = new LinkedList<String>();
			// for (TermUnit tu : termUnitList) {
			// linkedList.add(tu.getValue());
			// }
			// return linkedList;

			// List<AnalyzerResultPojo> linkedList = new
			// LinkedList<AnalyzerResultPojo>();
			List<AnalyzerResultPojo> linkedList = new LinkedList<AnalyzerResultPojo>();
			for (TermUnit tu : termUnitList) {
				linkedList.add(new AnalyzerResultPojo(tu.getValue(), tu
						.getNatureTermUnit().getTermNatureItem()
						.getNatureItem().getName()));
			}
			// System.out.println("linkedList---" + linkedList);
			return linkedList;
			// return linkedList.get(0);
		}
		return null;
	}

	public static List<AnalyzerResultPojo> convertTermUnitToPojoNoPos(
			List<TermUnit> termUnitList) {
		if (termUnitList != null && (!termUnitList.isEmpty())) {
			List<AnalyzerResultPojo> linkedList = new LinkedList<AnalyzerResultPojo>();
			for (TermUnit tu : termUnitList) {
				linkedList.add(new AnalyzerResultPojo(tu.getValue(), null));
			}
			return linkedList;
		}
		return null;
	}

}
