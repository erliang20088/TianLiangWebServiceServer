package com.zel.es.utils.es;

import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.zel.es.pojos.enums.SearchType;
import com.zel.es.pojos.enums.SortOrderEnum4Histogram;
import com.zel.es.pojos.search.SearchConditionItem;
import com.zel.es.pojos.search.SearchConditionPojo;
import com.zel.es.pojos.search.SearchConditionPojo.AdvancedSearchItem;
import com.zel.es.pojos.search.SearchConditionUnit;
import com.zel.es.pojos.statics.SearchAdvancedEnum;
import com.zel.es.utils.MyLogger;
import com.zel.es.utils.StringOperatorUtil;
 
/**   
 * 生成用于搜索的search source builder生成工具类 
 * 
 * @author zel
 *  
 */
public class SearchSourceBuilderGeneratorUtil {
	private static MyLogger logger = new MyLogger(
			SearchSourceBuilderGeneratorUtil.class);

	public static QueryBuilder getQueryBuilder(
			SearchConditionPojo searchConditionPojo) {
		List<SearchConditionUnit> searchConditionUnitList = searchConditionPojo
				.getSearchConditionUnitList();
		List<SearchConditionItem> searchConditionItemList = searchConditionPojo
				.getSearchConditionItemList();

		if (searchConditionPojo == null
				|| (StringOperatorUtil
						.isBlankCollection(searchConditionUnitList) && StringOperatorUtil
						.isBlankCollection(searchConditionItemList))) {
			return null;
		}
		// 布尔查询,这个是所有条件集合的汇总
		BoolQueryBuilder boolQueryBuilder_query_all = QueryBuilders.boolQuery();
		// 首先看简化搜索集合条件的拼装
		if (StringOperatorUtil.isNotBlankCollection(searchConditionItemList)) {
			BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderBySearchConditionItemList(searchConditionItemList);
			if (searchConditionPojo.getSearchType() == SearchType.AND) {
				boolQueryBuilder_query_all.must(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.OR) {
				boolQueryBuilder_query_all.should(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.NOT) {
				boolQueryBuilder_query_all.mustNot(boolQueryBuilder);
			}
		}
		// 封装search condition unit list
		if (StringOperatorUtil.isNotBlankCollection(searchConditionUnitList)) {
			BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderBySearchConditionUnitList(searchConditionUnitList);
			if (searchConditionPojo.getSearchType() == SearchType.AND) {
				boolQueryBuilder_query_all.must(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.OR) {
				boolQueryBuilder_query_all.should(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.NOT) {
				boolQueryBuilder_query_all.mustNot(boolQueryBuilder);
			}
		}

		return boolQueryBuilder_query_all;
	}

	public static QueryBuilder getQueryFilter(
			SearchConditionPojo searchConditionPojo) {
		List<SearchConditionUnit> searchConditionUnitList = searchConditionPojo
				.getSearchConditionUnitList();
		List<SearchConditionItem> searchConditionItemList = searchConditionPojo
				.getSearchConditionItemList();

		if (searchConditionPojo == null
				|| (StringOperatorUtil
						.isBlankCollection(searchConditionUnitList) && StringOperatorUtil
						.isBlankCollection(searchConditionItemList))) {
			return null;
		}
		// 布尔查询,这个是所有条件集合的汇总
		BoolQueryBuilder boolQueryBuilder_query_all = QueryBuilders.boolQuery();
		// 首先看简化搜索集合条件的拼装
		if (StringOperatorUtil.isNotBlankCollection(searchConditionItemList)) {
			BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderBySearchConditionItemList(searchConditionItemList);
			if (searchConditionPojo.getSearchType() == SearchType.AND) {
				boolQueryBuilder_query_all.must(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.OR) {
				boolQueryBuilder_query_all.should(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.NOT) {
				boolQueryBuilder_query_all.mustNot(boolQueryBuilder);
			}
		}
		// 封装search condition unit list
		if (StringOperatorUtil.isNotBlankCollection(searchConditionUnitList)) {
			BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderBySearchConditionUnitList(searchConditionUnitList);
			if (searchConditionPojo.getSearchType() == SearchType.AND) {
				boolQueryBuilder_query_all.must(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.OR) {
				boolQueryBuilder_query_all.should(boolQueryBuilder);
			} else if (searchConditionPojo.getSearchType() == SearchType.NOT) {
				boolQueryBuilder_query_all.mustNot(boolQueryBuilder);
			}
		}

		return boolQueryBuilder_query_all;
	}

	// 通过指定的搜索条件集合，封装成为bool query builder
	private static BoolQueryBuilder getBoolQueryBuilderBySearchConditionItemList(
			List<SearchConditionItem> searchConditionItemList) {
		// 首先查看原始的条件集合与拼装
		BoolQueryBuilder boolQueryBuilder_query = QueryBuilders.boolQuery();
		// 把list转化为es searchbuilder
		for (SearchConditionItem conditionItemPojo : searchConditionItemList) {
			// and查询构建
			if (conditionItemPojo.getSearchType() == SearchType.AND) {
				boolQueryBuilder_query = boolQueryBuilder_query
						.must(QueryBuilderProducerUtil
								.getQueryBuilder(conditionItemPojo));
			} else if (conditionItemPojo.getSearchType() == SearchType.RANGE) {// 范围查询构建
				boolQueryBuilder_query = boolQueryBuilder_query
						.must(QueryBuilderProducerUtil
								.getQueryBuilder(conditionItemPojo));
			} else if (conditionItemPojo.getSearchType() == SearchType.WILD) {
				boolQueryBuilder_query = boolQueryBuilder_query
						.must(QueryBuilderProducerUtil
								.getQueryBuilder(conditionItemPojo));
			} else if (conditionItemPojo.getSearchType() == SearchType.OR) {
				boolQueryBuilder_query = boolQueryBuilder_query
						.should(QueryBuilderProducerUtil
								.getQueryBuilder(conditionItemPojo));
			} else if (conditionItemPojo.getSearchType() == SearchType.NOT) {
				boolQueryBuilder_query = boolQueryBuilder_query
						.mustNot(QueryBuilderProducerUtil
								.getQueryBuilder(conditionItemPojo));
			}
		}
		return boolQueryBuilder_query;
	}

	/**
	 * 得到相封装成的filter
	 * 
	 * @return
	 */
	private static BoolFilterBuilder getBoolQueryFilterBySearchConditionItemList(
			List<SearchConditionItem> searchConditionItemList) {
		// 首先查看原始的条件集合与拼装
		BoolFilterBuilder boolFilterBuilder_filter = FilterBuilders
				.boolFilter();
		// 把list转化为es searchbuilder
		for (SearchConditionItem conditionPojo : searchConditionItemList) {
			// and查询构建
			if (conditionPojo.getSearchType() == SearchType.AND) {
				boolFilterBuilder_filter = boolFilterBuilder_filter
						.must(FilterBuilders.termFilter(
								conditionPojo.getName(),
								conditionPojo.getValue()));
			} else if (conditionPojo.getSearchType() == SearchType.RANGE) {// 范围查询构建
				boolFilterBuilder_filter = boolFilterBuilder_filter
						.must(FilterBuilders
								.rangeFilter(conditionPojo.getName())
								.from(conditionPojo.getFromObj())
								.to(conditionPojo.getToObj())
								.includeLower(conditionPojo.isIncludeLower())
								.includeUpper(conditionPojo.isIncludeHigher()));
			} else if (conditionPojo.getSearchType() == SearchType.OR) {
				boolFilterBuilder_filter = boolFilterBuilder_filter
						.should(FilterBuilders.termFilter(conditionPojo
								.getName(), conditionPojo.getValue().toString()));
			} else if (conditionPojo.getSearchType() == SearchType.NOT) {
				boolFilterBuilder_filter = boolFilterBuilder_filter
						.mustNot(FilterBuilders.termFilter(conditionPojo
								.getName(), conditionPojo.getValue().toString()));
			}
		}
		return boolFilterBuilder_filter;
	}

	// 通过指定的search condition unit list搜索条件集合，封装成为bool query builder
	private static BoolQueryBuilder getBoolQueryBuilderBySearchConditionUnitList(
			List<SearchConditionUnit> searchConditionUnitList) {
		// 首先查看原始的条件集合与拼装
		BoolQueryBuilder boolQueryBuilder_query_all = QueryBuilders.boolQuery();

		List<SearchConditionItem> searchConditionItemList = null;
		List<SearchConditionUnit> searchConditionUnitList_new = null;

		for (SearchConditionUnit searchConditionUnit : searchConditionUnitList) {
			// 先遍历独立条件集合，按must来处理
			if (StringOperatorUtil
					.isNotBlankCollection(searchConditionItemList = searchConditionUnit
							.getSearchConditionItemList())) {
				if (searchConditionUnit.getSearchType() == SearchType.AND) {
					boolQueryBuilder_query_all
							.must(getBoolQueryBuilderBySearchConditionItemList(searchConditionItemList));
				} else if (searchConditionUnit.getSearchType() == SearchType.OR) {
					boolQueryBuilder_query_all
							.should(getBoolQueryBuilderBySearchConditionItemList(searchConditionItemList));
				} else if (searchConditionUnit.getSearchType() == SearchType.NOT) {
					boolQueryBuilder_query_all
							.mustNot(getBoolQueryBuilderBySearchConditionItemList(searchConditionItemList));
				}
			}
			if (StringOperatorUtil
					.isNotBlankCollection(searchConditionUnitList_new = searchConditionUnit
							.getSearchConditionUnitList())) {
				if (searchConditionUnit.getSearchType() == SearchType.AND) {
					boolQueryBuilder_query_all
							.must(getBoolQueryBuilderBySearchConditionUnitList(searchConditionUnitList_new));
				} else if (searchConditionUnit.getSearchType() == SearchType.OR) {
					boolQueryBuilder_query_all
							.should(getBoolQueryBuilderBySearchConditionUnitList(searchConditionUnitList_new));
				} else if (searchConditionUnit.getSearchType() == SearchType.NOT) {
					boolQueryBuilder_query_all
							.mustNot(getBoolQueryBuilderBySearchConditionUnitList(searchConditionUnitList_new));
				}
			}
		}
		return boolQueryBuilder_query_all;
	}

	// 通过各种查询条件构建可查询对象SearchSourceBuilder,其逻辑类似于sql查询
	public static SearchSourceBuilder getSearchSourceBuilder(
			SearchConditionPojo searchConditionPojo) {
		// 首先声明该对象，既无论如会生成该对象,哪怕条件为空，则设置成match all即可
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		if (searchConditionPojo == null
				|| searchConditionPojo.isEmptyCondition()) {
			searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		} else {
			QueryBuilder boolQueryBuilder_query = getQueryBuilder(searchConditionPojo);
			searchSourceBuilder.query(boolQueryBuilder_query);
		}

		/**
		 * 在此处加入post filter,暂为range filter而添加
		 */
		FilterBuilder filterBuilder = getSearchPostFilter(searchConditionPojo);
		if (filterBuilder != null) {
			searchSourceBuilder.postFilter(filterBuilder);
		}
		// 测试部分--开始
		// searchSourceBuilder.noFields();
		// searchSourceBuilder.fields(new String[] { "uv", "pv" });

		// TermsFacetBuilder term_fb = FacetBuilders.termsFacet("pv_1");
		// term_fb.field("ad_ua_encode").;
		// CardinalityBuilder cb = AggregationBuilders.cardinality("agg_name")
		// .field("ad_ua_encode");
		// RangeFacetBuilder range_fb = FacetBuilders.rangeFacet("range_1");
		// range_fb.field("pv").addRange(10, 20);
		// searchSourceBuilder.facet(range_fb);
		// searchSourceBuilder.facet(term_fb);
		// searchSourceBuilder.aggregation(cb);
		// 测试部分--结束

		// 排序字段构建
		FieldSortBuilder fieldSortBuilder = getSortBuilder(searchConditionPojo);
		if (fieldSortBuilder != null) {
			searchSourceBuilder.sort(fieldSortBuilder);
		}

		// 关于返回字段的设置
		if (searchConditionPojo != null) {
			String[] field_array = searchConditionPojo.getField_array();
			if (field_array != null) {
				if (field_array.length == 0) {
					searchSourceBuilder.noFields();
				} else {
					/**
					 * fields过滤返回的是json数组，_source返回json串
					 */
					// searchSourceBuilder.fields(field_array);
					searchSourceBuilder.fetchSource(field_array, null);
				}
			}
		}

		// 设置高亮字段
		if (searchConditionPojo != null) {
			String[] high_light_field_array = searchConditionPojo
					.getHigh_light_field_array();
			if (high_light_field_array != null
					&& high_light_field_array.length > 0) {
				HighlightBuilder highlightBuilder = new HighlightBuilder();
				highlightBuilder.preTags("<span style=\"color:red\">");
				highlightBuilder.postTags("</span>");

				for (String field : high_light_field_array) {
					highlightBuilder.field(field);
					highlightBuilder.forceSource(true);
					highlightBuilder.fragmentSize(50000);
					// highlightBuilder.numOfFragments(10);
					// highlightBuilder.boundaryMaxScan(10);
				}
				searchSourceBuilder.highlight(highlightBuilder);
			}
		}

		/**
		 * 关于统计函数的设置，即AdvancedSearchItem集合的解析
		 */
		List<AbstractAggregationBuilder> aggBuilderList = getAggsBuilderList(searchConditionPojo);
		if (StringOperatorUtil.isNotBlankCollection(aggBuilderList)) {
			for (AbstractAggregationBuilder builder : aggBuilderList) {
				searchSourceBuilder.aggregation(builder);
			}
		}

		if (searchConditionPojo != null) {
			// 选择搜索命中结果集的起始偏移量及多少个元素
			searchSourceBuilder.from(searchConditionPojo.getStart());
			searchSourceBuilder.size(searchConditionPojo.getPageSize());
		} else {
			searchSourceBuilder.from(0);
			searchSourceBuilder.size(5);
		}

		logger.info(searchSourceBuilder.toString());
		// String json_result = searchSourceBuilder.toString().replace(
		// "\"fields\"", "\"_source\"");
		// logger.info(json_result);
		return searchSourceBuilder;
	}

	// 通过各种查询条件构建可查询对象SearchSourceBuilder,其逻辑类似于sql查询
	public static FilterBuilder getSearchPostFilter(
			SearchConditionPojo searchConditionPojo) {
		List<SearchConditionItem> searchConditionItemPostFilterList = null;
		if (searchConditionPojo == null
				|| (StringOperatorUtil
						.isBlankCollection(searchConditionItemPostFilterList = searchConditionPojo
								.getSearchPostFilterList()))) {
			return null;
		}
		FilterBuilder boolQueryBuilder_query = getBoolQueryFilterBySearchConditionItemList(searchConditionItemPostFilterList);

		return boolQueryBuilder_query;
	}

	// 通过各种查询条件构建可查询对象SearchSourceBuilder,其逻辑类似于sql查询
	public static FieldSortBuilder getSortBuilder(
			SearchConditionPojo searchConditionPojo) {
		List<SearchConditionItem> sortConditionItemList = null;
		if (searchConditionPojo == null
				|| (StringOperatorUtil
						.isBlankCollection(sortConditionItemList = searchConditionPojo
								.getSortConditionItemList()))) {
			return null;
		}
		if (StringOperatorUtil.isNotBlankCollection(sortConditionItemList)) {
			// 目前暂支持单一字段排序
			for (SearchConditionItem conditionPojo : sortConditionItemList) {
				if (conditionPojo.getSearchType() == SearchType.SORT) {
					FieldSortBuilder fieldSortBuilder = null;
					if (conditionPojo.getSortOrderEnum().toString()
							.equals(SortOrder.ASC.toString())) {
						fieldSortBuilder = SortBuilders.fieldSort(
								conditionPojo.getName()).order(SortOrder.ASC);
					} else {
						fieldSortBuilder = SortBuilders.fieldSort(
								conditionPojo.getName()).order(SortOrder.DESC);
					}
					return fieldSortBuilder;
				}
			}
		}

		return null;
	}

	// 通过各种查询条件构建可查询对象SearchSourceBuilder,其逻辑类似于sql查询
	public static List<AbstractAggregationBuilder> getAggsBuilderList(
			SearchConditionPojo searchConditionPojo) {
		List<AdvancedSearchItem> advancedItemList = null;
		if (searchConditionPojo == null
				|| (StringOperatorUtil
						.isBlankCollection(advancedItemList = searchConditionPojo
								.getAdvancedSearchItemList()))) {
			return null;
		}

		if (StringOperatorUtil.isNotBlankCollection(advancedItemList)) {
			List<AbstractAggregationBuilder> aggBuilderList = new LinkedList<AbstractAggregationBuilder>();
			for (AdvancedSearchItem advsearchItem : advancedItemList) {
				AbstractAggregationBuilder aggBuilder = createOneAggsBuilder(advsearchItem);
				if (aggBuilder != null) {
					aggBuilderList.add(aggBuilder);
				}
			}
			return aggBuilderList;
		}
		return null;
	}

	public static AbstractAggregationBuilder createOneAggsBuilder(
			AdvancedSearchItem advsearchItem) {
		if (advsearchItem == null) {
			return null;
		}
		// 防止死循环,但也仅能孩止1引1，大于等于3的环型的还没做检测
		if (advsearchItem.getAdvancedSearchItem() == advsearchItem) {
			logger.info("分组中出现了死循环,直接将下一个置空!");
			advsearchItem.setAdvancedSearchItem(null);
			// return null;
		}
		AbstractAggregationBuilder aggBuilder = null;
		TermsBuilder termsBuilder = null;
		DateHistogramBuilder dateHistogramBuilder = null;
		HistogramBuilder numberHistogramBuilder = null;

		if (advsearchItem.getAdvanceEnum() == SearchAdvancedEnum.Count_Uniq) {
			// 该字段目前只支持一个字段
			aggBuilder = AggregationBuilders.cardinality(
					advsearchItem.getAdvanced_name()).field(
					advsearchItem.getField());
			return aggBuilder;
		} else if (advsearchItem.getAdvanceEnum() == SearchAdvancedEnum.Group_By) {
			// 此处迭代准确性待考证
			termsBuilder = AggregationBuilders
					.terms(advsearchItem.getAdvanced_name())
					.field(advsearchItem.getField())
					.size(advsearchItem.getSize());
			if ((aggBuilder = createOneAggsBuilder(advsearchItem
					.getAdvancedSearchItem())) != null) {
				termsBuilder.subAggregation(aggBuilder);
			}
			return termsBuilder;
		} else if (advsearchItem.getAdvanceEnum() == SearchAdvancedEnum.Group_By_Date_histogram) {
			dateHistogramBuilder = AggregationBuilders
					.dateHistogram(advsearchItem.getAdvanced_name())
					.field(advsearchItem.getField())
					.interval(
							new DateHistogram.Interval(advsearchItem
									.getIntervalValue()))
					.minDocCount(advsearchItem.getMin_hit_number())
					.preZone("PRC");

			if (StringOperatorUtil.isNotBlank(advsearchItem.getFormat_key())) {
				dateHistogramBuilder.format(advsearchItem.getFormat_key());
			}
			if (advsearchItem.getMin_time_long() > 0) {
				dateHistogramBuilder.extendedBounds(
						advsearchItem.getMin_time_long(),
						advsearchItem.getMax_time_long());
			}

			if ((aggBuilder = createOneAggsBuilder(advsearchItem
					.getAdvancedSearchItem())) != null) {
				dateHistogramBuilder.subAggregation(aggBuilder);
			}
			return dateHistogramBuilder;
		} else if (advsearchItem.getAdvanceEnum() == SearchAdvancedEnum.Group_By_Number_histogram) {
			numberHistogramBuilder = AggregationBuilders
					.histogram(advsearchItem.getAdvanced_name())
					.field(advsearchItem.getField())
					.interval(advsearchItem.getInterval_number())
					.minDocCount(advsearchItem.getMin_hit_number());

			// 假定此处的min_value,max_value均为>=0的值，不会出现小于0的时候,否则要修改判断方式
			if (advsearchItem.getMin_number_long() > 0
					|| advsearchItem.getMax_number_long() > 0) {
				numberHistogramBuilder.extendedBounds(
						advsearchItem.getMin_number_long(),
						advsearchItem.getMax_number_long());
			}

			// 设置order by
			numberHistogramBuilder.order(getHistogramOrder(advsearchItem
					.getSortOrderEnum4Histogram()));

			if ((aggBuilder = createOneAggsBuilder(advsearchItem
					.getAdvancedSearchItem())) != null) {
				numberHistogramBuilder.subAggregation(aggBuilder);
			}
			return numberHistogramBuilder;
		}
		return null;
	}

	public static Order getHistogramOrder(
			SortOrderEnum4Histogram sortOrderEnum4Histogram) {
		Order order = null;
		switch (sortOrderEnum4Histogram) {
		case KEY_ASC:
			order = Histogram.Order.KEY_ASC;
			break;
		case KEY_DESC:
			order = Histogram.Order.KEY_DESC;
			break;
		case COUNT_ASC:
			order = Histogram.Order.COUNT_ASC;
			break;
		case COUNT_DESC:
			order = Histogram.Order.COUNT_DESC;
			break;
		default:
			order = null;
			break;
		}
		return order;
	}

}
