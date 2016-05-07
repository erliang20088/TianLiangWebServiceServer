package com.zel.es.utils.es;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.zel.es.pojos.enums.MatchType;
import com.zel.es.pojos.enums.SearchType;
import com.zel.es.pojos.search.SearchConditionItem;
  
/**  
 * query builder producer 
 *   
 * @author zel   
 * 
 */
public class QueryBuilderProducerUtil {
	public static QueryBuilder getQueryBuilder(SearchConditionItem conditionItem) {
		QueryBuilder queryBuilder = null;
		if (conditionItem.getSearchType() == SearchType.AND
				|| conditionItem.getSearchType() == SearchType.OR
				|| conditionItem.getSearchType() == SearchType.NOT) {
			if (conditionItem.getMatchType() == MatchType.Analyzer) {
				queryBuilder = QueryBuilders.queryString(
						conditionItem.getValue().toString()).field(
						conditionItem.getName());
			} else {
				queryBuilder = QueryBuilders.termQuery(conditionItem.getName(),
						conditionItem.getValue());
			}
		} else if (conditionItem.getSearchType() == SearchType.RANGE) {// 范围查询构建
			queryBuilder = QueryBuilders.rangeQuery(conditionItem.getName())
					.from(conditionItem.getFromObj())
					.to(conditionItem.getToObj())
					.includeLower(conditionItem.isIncludeLower())
					.includeUpper(conditionItem.isIncludeHigher());
		} else if (conditionItem.getSearchType() == SearchType.WILD) {
			queryBuilder = QueryBuilders.wildcardQuery(conditionItem.getName(),
					conditionItem.getValue().toString());
		}
		return queryBuilder;
	}
}
