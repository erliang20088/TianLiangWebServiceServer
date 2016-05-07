package com.zel.es.pojos;

/**
 * 为立勇大数据的关键词搜索准备的
 * 
 * @author zel
 * 
 */
public class TestSearchHotWord {
	private String search_keyword;
	private String host;
	private int uv;
	private int pv;
	private String search_keyword_not_analyzer;

	public String getSearch_keyword_not_analyzer() {
		return search_keyword_not_analyzer;
	}

	public void setSearch_keyword_not_analyzer(
			String search_keyword_not_analyzer) {
		this.search_keyword_not_analyzer = search_keyword_not_analyzer;
	}

	public int getPv() {
		return pv;
	}

	public void setPv(int pv) {
		this.pv = pv;
	}

	public int getUv() {
		return uv;
	}

	public void setUv(int uv) {
		this.uv = uv;
	}

	public String getHost() {
		return host;
	}

	public String getSearch_keyword() {
		return search_keyword;
	}

	public void setSearch_keyword(String search_keyword) {
		this.search_keyword = search_keyword;
	}

	public void setHost(String host) {
		this.host = host;
	}

}
