package com.zel.es.manager.nlp;
 
import java.util.LinkedList;
import java.util.List;

import com.zel.core.analyzer.StandardAnalyzer;
import com.zel.es.manager.other.AnalyzerResultConvertManager;
import com.zel.es.pojos.nlp.AnalyzerResultPojo;
import com.zel.interfaces.analyzer.Analyzer;

/**
 * 分词管理器
 * 
 * @author zel
 * 
 */
public class TianLiangAnalyzerManager {
	public static Analyzer analyzer = new StandardAnalyzer();

	public static List<AnalyzerResultPojo> getSplitResult(String src) {
		return AnalyzerResultConvertManager.convertTermUnitToPojoNoPos(analyzer
				.getSplitResult(src));
	}

	public static List<AnalyzerResultPojo> getSplitPOSResult(String src) {
		return AnalyzerResultConvertManager
				.convertTermUnitToPojoWithPos(analyzer
						.getSplitMergePOSResult(src));
	}

	public static void main(String[] args) {
		List<String> strlist = new LinkedList<String>();

		strlist.add("好人");
		strlist.add("微博已成社");
		strlist.add("社交网站");
		strlist.add("成功");
		strlist.add("失败");
		strlist.add("新浪网");
		strlist.add("搜狐科技");
		strlist.add("阿里巴巴");

		strlist.add("阿里巴巴");
		strlist.add("真正");
		strlist.add("理想与现实");

		strlist.add("的的打车");
		strlist.add("微软");
		strlist.add("一心一意");
		strlist.add("着了吗,?,1");
		strlist.add("民政局真的好吗?$#@.....");
		strlist.add("人人网");
		strlist.add("优酷土豆");
		strlist.add("顺风");
		strlist.add("顺风速递");
		strlist.add("顺风快递");
		strlist.add("打人");
		strlist.add("刺伤人");
		strlist.add("钓鱼岛");
		strlist.add("世");
		strlist.add("水");
		strlist.add("个子高");
		strlist.add("从小教我数学");
		strlist.add("从小");
		strlist.add("天天向上");
		strlist.add("中国好声音");
		strlist.add("梦想秀");
		strlist.add("正常情况下");
		strlist.add("东来顺");
		strlist.add("adf*123.af@");
		strlist.add("#9asd.3");
		strlist.add("随便测试一下");
		strlist.add("测试");
		strlist.add("天下无敌");
		strlist.add("中科院");
		strlist.add("梦里花落知多少");
		strlist.add("有限公司吗");
		strlist.add("怎么着");
		strlist.add("怎么着吧");
		strlist.add("看着办");
		strlist.add("梁山好汉");
		strlist.add("真不地道");
		strlist.add("真不地道啊");
		strlist.add("地道战");
		strlist.add("水浒传");
		strlist.add("西游记");
		strlist.add("天下无贼");
		strlist.add("坦诚为好");
		strlist.add("想你了");
		strlist.add("高凯");
		strlist.add("张华平");
		strlist.add("李开复");
		strlist.add("周二亮");
		strlist.add("杜峰");
		strlist.add("河北科技大学");
		strlist.add("北理工");
		strlist.add("清华大学");
		strlist.add("清华");
		strlist.add("北大大学");
		strlist.add("北大");
		strlist.add("清");
		strlist.add("华");
		strlist.add("北");
		strlist.add("大");
		strlist.add("业");
		strlist.add("中华");
		strlist.add("什么东西");
		strlist.add("非你莫属");
		strlist.add("非诚勿扰");
		strlist.add("天津卫视");
		strlist.add("找你妹");

		strlist.add("你");
		strlist.add("找");
		strlist.add("看");
		strlist.add("神奇");
		strlist.add("神");
		strlist.add("奇");

		strlist.add("中科点击");
		strlist.add("灵玖软件");
		strlist.add("高德软件");
		strlist.add("去哪儿网");
		strlist.add("腾讯");
		strlist.add("网易");

		strlist.add("易信");
		strlist.add("微博");
		strlist.add("微信");

		strlist.add("云居寺");
		strlist.add("水立方");

		strlist.add("微软 microsoft");
		strlist.add("百度");
		strlist.add("新浪");
		strlist.add("网易");
		strlist.add("腾讯");
		strlist.add("巨人");

		strlist.add("雅虎");
		strlist.add("水晶");
		strlist.add("日本");
		// strlist.add("microsoft software");

		// List<TermUnit> list = getSplitResult(src);
		// for (String str : strlist) {
		// List<AnalyzerResultPojo> list = getSplitPOSResult(str);
		// if (list != null) {
		// for (AnalyzerResultPojo term : list) {
		// // System.out.print(term.getValue() + ",");
		// System.out.println(term.getValue() + "/" + term.getNature() + ",");
		// }
		// } else {
		// System.out.println("分词结果为null");
		// }
		// }
	}
}
