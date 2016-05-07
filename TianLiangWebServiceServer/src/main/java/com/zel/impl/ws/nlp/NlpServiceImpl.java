package com.zel.impl.ws.nlp;

import java.util.LinkedList;
import java.util.List;

import javax.jws.WebService;

import org.ansj.app.keyword.Keyword;
import org.ansj.app.summary.pojo.Summary;

import com.zel.classfy.pojos.status.EmotionPolarStatus;
import com.zel.es.manager.nlp.TianLiangAnalyzerManager;
import com.zel.es.manager.nlp.TianLiangBodyExtractorManager;
import com.zel.es.manager.nlp.TianLiangEmotionCalcManager;
import com.zel.es.manager.nlp.TianLiangKeyWordExtractorManager;
import com.zel.es.manager.nlp.TianLiangSummaryExtractorManager;
import com.zel.es.manager.nlp.TianLiangThemeExtractorManager;
import com.zel.es.pojos.nlp.AnalyzerResultPojo;
import com.zel.es.utils.MyLogger;
import com.zel.es.utils.StringOperatorUtil;
import com.zel.iface.ws.nlp.INlpService;

/**
 * 任务服务实现类
 * 
 * @author zel
 * 
 */
@WebService(endpointInterface = "com.zel.iface.ws.nlp.INlpService", serviceName = "NlpService")
public class NlpServiceImpl implements INlpService {
	@Override
	public List<AnalyzerResultPojo> getSplit(String content) {
		return TianLiangAnalyzerManager.getSplitPOSResult(content);
	}

	@Override
	public List<String> getThemeKeyword(String content) {
		return TianLiangThemeExtractorManager.getThemeKeyword(content);
	}

	@Override
	public List<Keyword> getKeyword(String content) {
		List<Keyword> keywordList = TianLiangKeyWordExtractorManager
				.getKeyword(content);
		List<Keyword> list=new LinkedList<Keyword>();
		if(StringOperatorUtil.isNotBlankCollection(keywordList)){
			for(Keyword keyword:keywordList){
				list.add(keyword);
			}
		}
		return list;
	}

	@Override
	public Summary getSummary(int resultTxtLength, String title, String article) {
		Summary summary=TianLiangSummaryExtractorManager.getSummary(resultTxtLength,
				title, article);
//		if(summary!=null){
//			return summary.getSummary();
//		}
		return summary;
	}

	// 这里传入原始的html code即可
	@Override
	public String getBody(String content) {
		return TianLiangBodyExtractorManager.getBody(content);
	}

	@Override
	public EmotionPolarStatus getSentencePolar(String content) {
		return TianLiangEmotionCalcManager.getSentencePolar(content);
	}

	MyLogger logger = new MyLogger(NlpServiceImpl.class);

	@Override
	public String test() {
		// TODO Auto-generated method stub
		System.out.println("nlp service,it is here!");
		return null;
	}

}
