package com.zel.impl.ws.test;

import java.util.List; 

import javax.jws.WebService;

import com.zel.classfy.pojos.status.EmotionPolarStatus;
import com.zel.es.manager.nlp.TianLiangAnalyzerManager;
import com.zel.es.pojos.nlp.AnalyzerResultPojo;
import com.zel.es.utils.MyLogger;
import com.zel.iface.ws.test.ITestService;

/**  
 * 任务服务实现类  
 * 
 * @author zel
 * 
 */
@WebService(endpointInterface= "com.zel.iface.ws.test.ITestService", serviceName = "TestService")
public class TestServiceImpl implements ITestService {
	MyLogger logger = new MyLogger(TestServiceImpl.class);

	@Override
	public String test() {
		// TODO Auto-generated method stub
		System.out.println("nlp service,it is here!");
		return null;
	}

	@Override
	public List<AnalyzerResultPojo> getSplit(String content) {
		return TianLiangAnalyzerManager.getSplitPOSResult(content);
	}

	@Override
	public List<String> getThemeKeyword(String content) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getKeyword(String content) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSummary(int resultTxtLength, String title, String article) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getBody(String content) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EmotionPolarStatus getSentencePolar(String content) {
		// TODO Auto-generated method stub
		return null;
	}

}
