package com.zel.test.nlp;

import java.util.List;

import com.zel.entity.TermUnit;
import com.zel.es.manager.nlp.TianLiangAnalyzerManager;
import com.zel.es.pojos.nlp.AnalyzerResultPojo;

public class TestAnalyzer {
	public static void main(String[] args) {
		TianLiangAnalyzerManager tianLiangAnalyzerManager = new TianLiangAnalyzerManager();
		String src = "成功者天下邦";
//		List<AnalyzerResultPojo> termList = tianLiangAnalyzerManager
//				.getSplitPOSResult(src);
//		for(AnalyzerResultPojo tu:termList){
//			System.out.println(tu.getValue());
//		}
		
	}
}
