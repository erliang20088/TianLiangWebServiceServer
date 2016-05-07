package com.zel.es.manager.es;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.zel.es.pojos.enums.IntervalUnitEnum4DateHistogram;
import com.zel.es.pojos.enums.SearchType;
import com.zel.es.pojos.enums.SortOrderEnum;
import com.zel.es.pojos.enums.SortOrderEnum4Histogram;
import com.zel.es.pojos.search.SearchConditionItem;
import com.zel.es.pojos.search.SearchConditionPojo;
import com.zel.es.pojos.search.SearchConditionPojo.AdvancedSearchItem;
import com.zel.es.pojos.search.SearchConditionUnit;
import com.zel.es.pojos.statics.SearchAdvancedEnum;
import com.zel.es.pojos.statics.StaticValue4SearchCondition;
import com.zel.es.utils.ESSearchOperatorUtil;
import com.zel.es.utils.MyLogger;
import com.zel.es.utils.StringOperatorUtil;
import com.zel.es.utils.es.SearchSourceBuilderGeneratorUtil;

/**
 * es搜索管理器
 * 
 * @author zel
 * 
 */
public class ESSearchManager {
	private static MyLogger logger = new MyLogger(ESSearchManager.class);

	// 两个工具类的静态声明
	private static ESSearchOperatorUtil<Object> searchOperatorUtil = new ESSearchOperatorUtil<Object>();

	public static String search4RetJson(SearchConditionPojo searchConditionPojo) {
		if (StringOperatorUtil.isBlank(searchConditionPojo.getIndexName())) {
			return "{'error':'please input the index name!'}";
		}
		SearchSourceBuilder searchSourceBuilder = SearchSourceBuilderGeneratorUtil
				.getSearchSourceBuilder(searchConditionPojo);

		String jsonStr = searchOperatorUtil.search4RetJson(
				searchConditionPojo.getIndexName(),
				searchConditionPojo.getIndexType(), searchSourceBuilder);

		logger.info("client search one time, ret json");

		return jsonStr;
	}

	public static List<String> search4IndexIdList(
			SearchConditionPojo searchConditionPojo) {
		if (StringOperatorUtil.isBlank(searchConditionPojo.getIndexName())) {
			logger.info("{'error':'please input the index name!'}");
			return null;
		}
		SearchSourceBuilder searchSourceBuilder = SearchSourceBuilderGeneratorUtil
				.getSearchSourceBuilder(searchConditionPojo);

		List<String> idList = searchOperatorUtil.search4IndexIdList(
				searchConditionPojo.getIndexName(),
				searchConditionPojo.getIndexType(), searchSourceBuilder);

		logger.info("client search one time, ret json");

		return idList;
	}

	public static List<Object> search4RetObj(
			SearchConditionPojo searchConditionPojo) {
		SearchSourceBuilder searchSourceBuilder = SearchSourceBuilderGeneratorUtil
				.getSearchSourceBuilder(searchConditionPojo);
		List<Object> resultList = searchOperatorUtil.search4RetObj(
				"ad_crowd_base_info", "ad_crowd", searchSourceBuilder,
				Object.class);

		logger.info("client search one time, ret object");

		return resultList;
	}

	public static void main(String[] args) {
		SearchConditionPojo searchConditionPojo = new SearchConditionPojo();
		searchConditionPojo.setSearchType(SearchType.AND);
		searchConditionPojo 
				.setIndexName(StaticValue4SearchCondition.index_name_yuqing);
		searchConditionPojo
				.setIndexType(StaticValue4SearchCondition.type_name_portals_web_data);
		searchConditionPojo.setStart(0);
		searchConditionPojo.setPageSize(5);

		/**
		 * 搜索条件封装---开始
		 */
		/************************************************
		 * search unit的加入---开始
		 */
		List<SearchConditionItem> sortConditionList = new ArrayList<SearchConditionItem>();
		List<SearchConditionItem> searchConditionItemList4Unit = null;
		SearchConditionItem searchConditionItem = null;
		
		// 第一个unit
		List<SearchConditionUnit> searchConditionUnitList_outer = new ArrayList<SearchConditionUnit>();

		SearchConditionUnit searchConditionUnit_inner_1 = new SearchConditionUnit();
		searchConditionItemList4Unit = new ArrayList<SearchConditionItem>();
		// searchConditionUnit.setSearchType(SearchType.OR);
		searchConditionUnit_inner_1.setSearchType(SearchType.OR);
		// searchConditionUnitList=new ArrayList<SearchConditionUnit>();
		searchConditionItemList4Unit = new ArrayList<SearchConditionItem>();

		searchConditionItem = new SearchConditionItem();
		searchConditionItem.setSearchType(SearchType.AND);
		// searchConditionItem.setMatchType(MatchType.Analyzer);
		// searchConditionItem.setMatchType(MatchType.Not_Analyzer);
		// searchConditionItem.setName("host");
		searchConditionItem.setName("transmit_number");
		searchConditionItem.setValue(3);
		searchConditionItemList4Unit.add(searchConditionItem);
		searchConditionUnit_inner_1
				.setSearchConditionItemList(searchConditionItemList4Unit);

		searchConditionUnitList_outer.add(searchConditionUnit_inner_1);

		// searchConditionPojo
		// .setSearchConditionUnitList(searchConditionUnitList_outer);

		// searchConditionPojo.setSearchConditionUnitList(null);
		/************************************************
		 * search unit的加入--结束
		 */

		/**
		 * 排序字段的封装---开始
		 */
		SearchConditionItem sortConditionItem = new SearchConditionItem();
		// searchConditionItem.setName("src_ip");
		sortConditionItem.setName("publish_time");
		// sortConditionItem.setName("discuss_number");
		// searchConditionItem.setName("keyword");
		// searchConditionItem.setValue("钻戒戒托");
		// searchConditionItem.setName("dt");
		// searchConditionItem.setName("crowd_id");
		sortConditionItem.setSearchType(SearchType.SORT);
		sortConditionItem.setSortOrderEnum(SortOrderEnum.DESC);
		sortConditionList.add(sortConditionItem);

		// searchConditionPojo.setField_array(new String[] { "publish_time" });
		// searchConditionPojo.setHigh_light_field_array(new String[] { "body",
		// "summary" });

		// searchConditionPojo.setSortConditionItemList(sortConditionList);
		/**
		 * filter 字段封装
		 */
		// List<SearchConditionItem> searchConditionItemList4Filter = new
		// ArrayList<SearchConditionItem>();
		// searchConditionItem = new SearchConditionItem();
		// searchConditionItem.setSearchType(SearchType.RANGE);
		// searchConditionItem.setName("uv");
		// searchConditionItem.setFromObj(0);
		// searchConditionItem.setToObj(10000);
		// searchConditionItemList4Filter.add(searchConditionItem);
		//
		// searchConditionPojo
		// .setSearchPostFilterList(searchConditionItemList4Filter);
		/**
		 * 排序字段的封装---结束
		 */
		// searchConditionPojo.setField_array(new String[] { "f1", "f2" });

		/**
		 * advanced search 封装
		 */
		List<AdvancedSearchItem> adItemList = new LinkedList<AdvancedSearchItem>();

		AdvancedSearchItem advancedSearchItem = new AdvancedSearchItem();
		advancedSearchItem.setAdvanced_name("uniq_count");
		advancedSearchItem.setAdvanceEnum(SearchAdvancedEnum.Count_Uniq);
		advancedSearchItem.setField("person_id");
		// adItemList.add(advancedSearchItem);
		// adItemList.add(advancedSearchItem);

		advancedSearchItem = new AdvancedSearchItem();
		advancedSearchItem.setAdvanced_name("group_by");
		advancedSearchItem.setAdvanceEnum(SearchAdvancedEnum.Group_By);
		advancedSearchItem.setField("title");
		// advancedSearchItem.setSize(1);
		adItemList.add(advancedSearchItem);

		AdvancedSearchItem advancedSearchItem2 = new AdvancedSearchItem();
		advancedSearchItem2.setAdvanced_name("group_by_2");
		advancedSearchItem2.setAdvanceEnum(SearchAdvancedEnum.Group_By);
		advancedSearchItem2.setField("author");
		advancedSearchItem2.setSize(1);
		advancedSearchItem2.setAdvancedSearchItem(advancedSearchItem);

		AdvancedSearchItem advancedSearchItem3 = new AdvancedSearchItem();
		advancedSearchItem3.setAdvanced_name("group_by_3");
		advancedSearchItem3
				.setAdvanceEnum(SearchAdvancedEnum.Group_By_Number_histogram);
		advancedSearchItem3.setField("media_type");
		advancedSearchItem3.setMax_number_long(10);

		// advancedSearchItem3.setAdvancedSearchItem(advancedSearchItem2);

		// adItemList.add(advancedSearchItem3);

		// 添加date history gram统计
		AdvancedSearchItem advancedSearchItem4 = new AdvancedSearchItem();
		advancedSearchItem4.setAdvanced_name("date_histogra_1");
		advancedSearchItem4
				.setAdvanceEnum(SearchAdvancedEnum.Group_By_Date_histogram);
		advancedSearchItem4.setField("publish_time_long");
		advancedSearchItem4.setSize(1);
		advancedSearchItem4.setFormat_key("yyyy-MM-dd hh:mm:ss");
		advancedSearchItem4
				.setSortOrderEnum4Histogram(SortOrderEnum4Histogram.COUNT_DESC);
		advancedSearchItem4.setIntervalUnit(IntervalUnitEnum4DateHistogram.DAY);

		// advancedSearchItem4.setAdvancedSearchItem(advancedSearchItem3);
		// advancedSearchItem3.setAdvancedSearchItem(advancedSearchItem4);

		// adItemList.add(advancedSearchItem4);

		searchConditionPojo.setAdvancedSearchItemList(adItemList);

		// searchConditionPojo.setField_array(new String[] { "publish_time",
		// "person_id", "body" });

		SearchSourceBuilder sb = SearchSourceBuilderGeneratorUtil
				.getSearchSourceBuilder(searchConditionPojo);
		// System.out.println(sb.toString());

		// get _id value test
		// List<String> resultList = search4IndexIdList(searchConditionPojo);
		// System.out.println(resultList);

		/**
		 * range filter构造
		 */
		// SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		// searchSourceBuilder.from(0);
		// searchSourceBuilder.size(10);
		// range package
		// RangeFilterBuilder rangeFilterBuilder = FilterBuilders
		// .rangeFilter("uv").from(0).to(10000);
		// searchSourceBuilder.postFilter(rangeFilterBuilder);
		// // condition package
		// QueryBuilder queryBuilder=QueryBuilders.termQuery("keyword","中国");
		// searchSourceBuilder.query(queryBuilder);

		// System.out.println(sb.toString());

	}
}
