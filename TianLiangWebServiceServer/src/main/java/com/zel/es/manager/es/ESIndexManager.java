package com.zel.es.manager.es;

import io.searchbox.core.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;

import com.zel.es.manager.es.index.mq.IndexMessageConsumer;
import com.zel.es.manager.es.index.mq.IndexMessageQueueManager;
import com.zel.es.manager.thread.ThreadManager;
import com.zel.es.pojos.IndexTaskPojo;
import com.zel.es.pojos.enums.SearchType;
import com.zel.es.pojos.index.CrawlData4PortalSite;
import com.zel.es.pojos.search.SearchConditionItem;
import com.zel.es.pojos.search.SearchConditionPojo;
import com.zel.es.pojos.statics.StaticValue4SearchCondition;
import com.zel.es.utils.ESIndexOperatorUtil;
import com.zel.es.utils.FileOperatorUtil;
import com.zel.es.utils.IOUtil;
import com.zel.es.utils.MyLogger;
import com.zel.es.utils.StaticValue;
import com.zel.es.utils.StringOperatorUtil;
import com.zel.es.utils.StringToObjectConvertUtil;
import com.zel.es.utils.SystemParas;
import com.zel.es.utils.es.SearchSourceBuilderGeneratorUtil;

/**
 * es搜索管理器
 * 
 * @author zel
 * 
 */
public class ESIndexManager {
	private static MyLogger logger = new MyLogger(ESIndexManager.class);
	private static ESIndexOperatorUtil esIndexOperatorUtil = new ESIndexOperatorUtil();

	private IndexMessageQueueManager indexMessageQueueManager;

	public ESIndexManager(IndexMessageQueueManager indexMessageQueueManager) {
		this.indexMessageQueueManager = indexMessageQueueManager;
	}

	// 将所有ESOperatorUtil工具类的所有操作均通过ESIndexManager来完成，不直接引用ESOperatorUtil来搞
	public void addIndexToES(String indexName, String typeName,
			List<Index> indexList) {
		esIndexOperatorUtil.addIndexToES(indexName, typeName, indexList);
	}

	// 将任意对象要索引的数据对象，转化为可索引地象
	private static List<Index> convertToIndexList(List pojoList) {
		List<Index> indexList = new ArrayList<Index>();
		Index index = null;
		for (Object obj : pojoList) {
			index = new Index.Builder(obj).build();
			indexList.add(index);
		}
		return indexList;
	}

	// 批量添加索引
	public void addBatchIndex(String indexName, String indexType, List pojoList) {
		if (StringOperatorUtil.isBlankCollection(pojoList)) {
			return;
		}
		// 判断是否还需要往资源池中追加
		while (indexMessageQueueManager.isShouldWait()) {
			try {
				logger.info("index to do task pool is to threshold,the producer threads will sleep "
						+ (SystemParas.add_index_sleep_interval_time / 1000)
						+ " s");
				Thread.sleep(SystemParas.add_index_sleep_interval_time);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		List<Index> indexList = convertToIndexList(pojoList);
		indexMessageQueueManager.pushToDoList(new IndexTaskPojo(indexName,
				indexType, indexList));
	}

	// 批量添加索引
	public static void addBatchIndexByString(String indexName,
			String indexType, List<String> lineStringList) {
		if (StringOperatorUtil.isBlankCollection(lineStringList)) {
			return;
		}
		List<Index> indexList = new ArrayList<Index>();
		Index index = null;
		// AdCrowdBaseInfoPojo adCrowdBaseInfoPojo = null;
		for (String line : lineStringList) {
			indexList.add(index);
		}
		esIndexOperatorUtil.addIndexToES(indexName, indexType, indexList);
	}

	// 批量添加索引
	public static void addBatchIndexByObj(String indexName, String indexType,
			List pojoList) {
		if (StringOperatorUtil.isBlankCollection(pojoList)) {
			return;
		}
		List<Index> indexList = new ArrayList<Index>();
		Index index = null;
		for (Object pojo : pojoList) {
			index = new Index.Builder(pojo).build();
			indexList.add(index);
		}
		esIndexOperatorUtil.addIndexToES(indexName, indexType, indexList);
	}

	public static void startNewIndexProcessTask(String indexName,
			String indexType, String rootPath, Class indexPojoClass) {
		// 初始化索引数据资源池对象
		IndexMessageQueueManager indexMessageQueueManager = new IndexMessageQueueManager();
		// 传入有生产数据的管理对象中
		ESIndexManager esIndexManager = new ESIndexManager(
				indexMessageQueueManager);
		// 添加索引
		esIndexManager.addRootDataToIndex(indexName, indexType, rootPath,
				indexPojoClass);
	}

	public void addRootDataToIndex(String indexName, String indexType,
			String rootPath, Class indexPojoClass) {
		/**
		 * 开启索引线程的消费者类,开启指定个数的消费者线程类
		 */
		IndexMessageConsumer indexMessageConsumer = null;
		for (int i = 0; i < SystemParas.add_index_multi_threads_consumer_number; i++) {
			indexMessageConsumer = new IndexMessageConsumer(i + 1,
					this.indexMessageQueueManager, this);
			new Thread(indexMessageConsumer).start();
		}

		// 传入配置文件中关于多线程方式的参数
		BigDataRunnable bigDataRunnable = new BigDataRunnable(
				SystemParas.add_index_by_multi_threads_setup_by_file_or_txtLines,
				indexName, indexType, rootPath, indexPojoClass,
				this.indexMessageQueueManager, this);
		new Thread(bigDataRunnable).start();
	}

	// 根据query去删除索引信息
	public static void deleteByQuery(SearchConditionPojo searchConditionPojo) {
		if (searchConditionPojo == null
				|| StringOperatorUtil.isBlank(searchConditionPojo
						.getIndexName())) {
			return;
		}
		QueryBuilder queryBuilder = SearchSourceBuilderGeneratorUtil
				.getQueryBuilder(searchConditionPojo);
		esIndexOperatorUtil.deleteIndexByQuery(
				searchConditionPojo.getIndexName(),
				searchConditionPojo.getIndexType(), queryBuilder);
	}

	public static void deleteByIndexName(String indexName) {
		esIndexOperatorUtil.deleteByIndexName(indexName);
	}

	// 根据id来删除对应的索引数据
	public static void updateIndexByIds(String indexName, String typeName,
			List<String> idsList, Map<String, Object> newFieldsMap) {
		if (StringOperatorUtil.isNotBlankCollection(idsList)) {
			for (String id : idsList) {
				esIndexOperatorUtil.updateIndexByIds(indexName, typeName, id,
						newFieldsMap);
			}
		}
	}

	// 根据conditionPojo来查询出旧的index然后update旧索引，最插入新的索引数据
	public static void updateAndSaveIndex(
			SearchConditionPojo searchConditionPojo, List indexList,
			Map<String, Object> newFieldsMap) {
		if (searchConditionPojo != null) {
			List<String> idsList = ESSearchManager
					.search4IndexIdList(searchConditionPojo);
			String indexName = searchConditionPojo.getIndexName();
			String typeName = searchConditionPojo.getIndexType();

			if (StringOperatorUtil.isNotBlankCollection(idsList)) {
				for (String id : idsList) {
					esIndexOperatorUtil.updateIndexByIds(indexName, typeName,
							id, newFieldsMap);
				}
			}
			addBatchIndexByObj(indexName, typeName, indexList);
		}
	}

	// 修改emotion时，将此时的searchConditionPojo的"_id"设置成要改的_id即可
	public static void updateEmotionIndex4WebDoc(
			SearchConditionPojo searchConditionPojo,
			Map<String, Object> newFieldsMap) {
		if (searchConditionPojo != null) {
			List<String> idsList = ESSearchManager
					.search4IndexIdList(searchConditionPojo);
			String indexName = searchConditionPojo.getIndexName();
			String typeName = searchConditionPojo.getIndexType();

			if (StringOperatorUtil.isNotBlankCollection(idsList)) {
				for (String id : idsList) {
					esIndexOperatorUtil.updateIndexByIds4Emotion(indexName,
							typeName, id, newFieldsMap);
				}
			}
			// 此处为直接修改，不进行数据的更新
			// addBatchIndexByObj(indexName, typeName, indexList);
		}
	}

	public static void deleteIndexByIndexOrType(String indexName,
			String typeName) {
		esIndexOperatorUtil.deleteIndexByIndexOrType(indexName, typeName);
	}

	public static void createIndex(String indexName) {
		esIndexOperatorUtil.createIndex(indexName);
	}

	/**
	 * 接收文件名称，并取文件中行列表并添加到索引服务器端的专用线程类
	 * 
	 * @author zel
	 * 
	 */
	class BigDataRunnable implements Runnable {
		// 多线程的运行方式，是byFile,还是byTxtLines
		private int multi_threads_method;
		private String indexName;
		private String indexType;
		private String rootPath;
		// 标志要将读出的文件行解析成哪种pojo对象
		private Class indexObjPojoClass;
		private IndexMessageQueueManager indexMessageQueueManager;
		private ESIndexManager esIndexManager;

		public BigDataRunnable(int multi_threads_method, String indexName,
				String indexType, String rootPath, Class indexObjPojoClass,
				IndexMessageQueueManager indexMessageQueueManager,
				ESIndexManager esIndexManager) {
			this.multi_threads_method = multi_threads_method;
			this.indexName = indexName;
			this.indexType = indexType;
			this.rootPath = rootPath;
			this.indexObjPojoClass = indexObjPojoClass;

			this.indexMessageQueueManager = indexMessageQueueManager;
			this.esIndexManager = esIndexManager;
		}

		@Override
		public void run() {
			/**
			 * 直接读根数据文件夹
			 */
			List<String> fileArray = FileOperatorUtil.getAllFilePathList(
					this.rootPath, null);
			long total_size_finished = 0;
			int file_number_finished = 0;

			int list_size;
			int threads_number_count = 0;

			int sleep_millseconds = 3 * SystemParas.add_index_sleep_interval_time;

			int total_file_size = 0;
			if (fileArray != null) {
				total_file_size = fileArray.size();
			}

			ThreadManager threadManager = new ThreadManager("IndexThread");
			if (StringOperatorUtil.isNotBlankCollection(fileArray)) {
				logger.info("root data include " + fileArray.size() + " files");

				for (String filePath : fileArray) {
					// 判断多线程开启的方式,0为按照文件方式启动多线程添加索引，1代表按文本行来一批开启一个线程
					if (this.multi_threads_method == 0) {// 按文件数，并行开启文件数量个线程来添加索引
						// 暂时不做限制开启线程的个数
						while (true) {
							// 线程数量,即线程id
							// 不再是以固定的文本数量来决定线程的数量，由配置来确定，并随着线程的结速加入新的线程
							if (threadManager.canAddNewThread()) {
								threads_number_count++;
								BigDataRunnableByFile bigDataRunnableByFile = new BigDataRunnableByFile(
										threads_number_count, indexName,
										indexType, filePath, indexObjPojoClass,
										this.esIndexManager);
								threadManager
										.addRunnableAndStart(bigDataRunnableByFile);
								break;
							}
							try {
								logger.info("index main thread controler will sleep "
										+ sleep_millseconds / 1000 + " s");
								Thread.sleep(sleep_millseconds);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					} else if (this.multi_threads_method == 1) {// 按文本行
						int begin_line_number = 0;
						List<String> txtLinesList = null;
						// 按批量读取出文件中的所有记录
						while (begin_line_number <= SystemParas.add_index_total_max_size) {
							// 读取出指定的文件中的内容
							txtLinesList = IOUtil
									.getLineArrayFromFile(
											filePath,
											StaticValue.default_encoding,
											begin_line_number,
											Math.min(
													SystemParas.add_index_batch_size,
													SystemParas.add_index_total_max_size));

							// 说明已经全部添加完毕了
							if ((list_size = txtLinesList.size()) <= 0) {
								logger.info("in the file,the all line string is finished to the index!");
								break;
							}
							/**
							 * 如果是按文件来开启线程，则实际当中开启。如果按文本行来添加索引的话，就直接添加到资池池中，
							 * 不单开启线程来添加。
							 */
							// try {
							// List<Object> indexObjList =
							// StringToObjectConvertUtil
							// .convertToObjectPojoList(txtLinesList,
							// this.indexObjPojoClass
							// .newInstance());
							// ESIndexManager.addBatchIndex(this.indexName,
							// this.indexType, indexObjList);
							// } catch (Exception e) {
							// e.printStackTrace();
							// logger.info("index producer threads occur exception,"
							// + e.getLocalizedMessage());
							// }
							// 这里的线程数据量会多一些，每一批都会开一个新的线程
							threads_number_count++;
							startThreadsByTxtLines(threads_number_count,
									indexName, indexType, txtLinesList,
									this.indexObjPojoClass, this.esIndexManager);

							begin_line_number = begin_line_number + list_size;
							total_size_finished += list_size;
							logger.info("total_size_finished----"
									+ total_size_finished);

							// 新开的线程数，每10个休息一下
							if (threads_number_count % 10 == 0) {
								try {
									logger.info("threads_number_count%10==0,will sleep for procedure thread!");
									Thread.sleep(SystemParas.add_index_sleep_interval_time);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
						// 在此处把文本文件给删除掉
						FileOperatorUtil.removeFile(filePath);

						// 文件完成计数
						file_number_finished++;
						logger.info("file_number_finished----"
								+ file_number_finished);
					}

					// 每个文件结束后休息n秒
					// try {
					// logger.info("add index thread will sleep "
					// + SystemParas.add_index_sleep_interval_time
					// / 1000 + " 秒");
					// Thread.sleep(SystemParas.add_index_sleep_interval_time);
					// } catch (Exception e) {
					// e.printStackTrace();
					// }
				}
			} else {
				logger.info("root data include 0 files,add root data process will exit!");
			}

			logger.info("the all index tasks is over!");
		}

		// 暂废弃
		// private void startThreadsByFile(int threadId, String indexName,
		// String indexType, String filePath, Class
		// indexObjPojoClass,ESIndexManager esIndexManager) {
		// BigDataRunnableByFile bigDataRunnableByFile = new
		// BigDataRunnableByFile(
		// threadId, indexName, indexType, filePath,
		// indexObjPojoClass,esIndexManager);
		// new Thread(bigDataRunnableByFile).start();
		// }

		private void startThreadsByTxtLines(int threadId, String indexName,
				String indexType, List<String> txtLinesList,
				Class indexObjPojoClass, ESIndexManager esIndexManager) {
			BigDataRunnableByTxtLines bigDataRunnableByTxtLines = new BigDataRunnableByTxtLines(
					threadId, indexName, indexType, txtLinesList,
					indexObjPojoClass, esIndexManager);
			new Thread(bigDataRunnableByTxtLines).start();
		}
	}

	class BigDataRunnableByFile implements Runnable {
		private String indexName;
		private String indexType;
		private String filePath;
		// 线程所属的线程编号id
		private int threadId;
		// 标志要将读出的文件行解析成哪种pojo对象,能过newInstance反射可得
		private Object indexObjClassOwnToObject;
		private String prefix_thread_log_info;

		private ESIndexManager esIndexManager;

		public BigDataRunnableByFile(int threadId, String indexName,
				String indexType, String filePath, Class indexObjPojoClass,
				ESIndexManager esIndexManager) {
			this.threadId = threadId;
			this.indexName = indexName;
			this.indexType = indexType;
			this.filePath = filePath;
			// 这样只需要一次转换即可得
			try {
				this.indexObjClassOwnToObject = indexObjPojoClass.newInstance();
			} catch (Exception e) {
				e.printStackTrace();
			}
			this.prefix_thread_log_info = "ByFile_threads_" + this.threadId
					+ "#";

			this.esIndexManager = esIndexManager;
		}

		@Override
		public void run() {
			long total_size_finished = 0;
			int file_number_finished = 0;

			int list_size;
			if (StringOperatorUtil.isNotBlank(this.filePath)) {
				int begin_line_number = 0;
				// 按批量读取出文件中的所有记录
				while (begin_line_number <= SystemParas.add_index_total_max_size) {
					// 读取出指定的文件中的内容
					List<String> txtContent = IOUtil.getLineArrayFromFile(
							this.filePath, StaticValue.default_encoding,
							begin_line_number, Math.min(
									SystemParas.add_index_batch_size,
									SystemParas.add_index_total_max_size));

					// 说明已经全部添加完毕了
					if ((list_size = txtContent.size()) <= 0) {
						logger.info(this.prefix_thread_log_info
								+ "in the file,the all line string is finished to the index!");
						break;
					}

					List<Object> indexObjList = StringToObjectConvertUtil
							.convertToObjectPojoList(txtContent,
									this.indexObjClassOwnToObject);

					esIndexManager.addBatchIndex(this.indexName,
							this.indexType, indexObjList);
					begin_line_number = begin_line_number + list_size;

					total_size_finished += list_size;
					logger.info(this.prefix_thread_log_info
							+ "total_size_finished----" + total_size_finished);
				}

				file_number_finished++;
				logger.info(this.prefix_thread_log_info
						+ "file_number_finished----" + file_number_finished);

				// 每个文件结束后休息n秒
				try {
					logger.info(this.prefix_thread_log_info
							+ "add index thread will sleep "
							+ SystemParas.add_index_sleep_interval_time / 1000
							+ " 秒");
					Thread.sleep(SystemParas.add_index_sleep_interval_time);
				} catch (Exception e) {
					e.printStackTrace();
				}
				// 在此处把文本文件给删除掉
				FileOperatorUtil.removeFile(filePath);
			} else {
				logger.info(this.prefix_thread_log_info
						+ "root data include 0 files,add root data process will exit!");
			}
			logger.info(this.prefix_thread_log_info
					+ ",the index thread is running over,will exit!");
		}
	}

	// 通过一批一批非阻塞式的去添加文本行达到多线程添加索引数据的目的
	class BigDataRunnableByTxtLines implements Runnable {
		private String indexName;
		private String indexType;
		// 线程所属的线程编号id
		private int threadId;
		// 一次一次传过来的批量文本数据
		private List<String> txtLinesList;
		// 标志要将读出的文件行解析成哪种pojo对象,能过newInstance反射可得
		private Object indexObjClassOwnToObject;
		private String prefix_thread_log_info;

		private ESIndexManager esIndexManager;

		public BigDataRunnableByTxtLines(int threadId, String indexName,
				String indexType, List<String> txtLinesList,
				Class indexObjPojoClass, ESIndexManager esIndexManager) {
			this.threadId = threadId;
			this.indexName = indexName;
			this.indexType = indexType;
			this.txtLinesList = txtLinesList;
			// 这样只需要一次转换即可得
			try {
				this.indexObjClassOwnToObject = indexObjPojoClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
			this.prefix_thread_log_info = "ByTxtLines_threads_" + this.threadId
					+ "#";

			this.esIndexManager = esIndexManager;
		}

		@Override
		public void run() {
			if (StringOperatorUtil.isNotBlankCollection(this.txtLinesList)) {
				// 按批量读取出文件中的所有记录
				if (txtLinesList.size() <= 0) {
					logger.info(this.prefix_thread_log_info
							+ "in the txt lines list,the all line string is finished to the index!");
					return;
				}
				List<Object> indexObjList = StringToObjectConvertUtil
						.convertToObjectPojoList(txtLinesList,
								this.indexObjClassOwnToObject);
				esIndexManager.addBatchIndex(this.indexName, this.indexType,
						indexObjList);
			} else {
				logger.info(this.prefix_thread_log_info
						+ "txt lines list is null or empty!");
			}
		}
	}

	public static void createIndex(String indexName, String indexType) {
		esIndexOperatorUtil.createIndex(indexName);
	}

	public static void main(String[] args) {
		IndexMessageQueueManager indexMessageQueueManager = null;
		ESIndexManager esIndexManager = new ESIndexManager(
				indexMessageQueueManager);

		String indexName = "yuqing_studio";
		// String typeName =
		// StaticValue4SearchCondition.type_name_portals_web_data;
		String typeName = "portals_web_data";

		// Map<String, Object> newFieldsMap = new HashMap<String, Object>();
		// newFieldsMap.put("is_new", 1);
		
		CrawlData4PortalSite crawlData4PortalSite = new CrawlData4PortalSite();
		crawlData4PortalSite.setUrl("http://www.baidu.com/3");
		crawlData4PortalSite.setAuthor("韩寒");
		crawlData4PortalSite.setTitle("自定义标题");
		crawlData4PortalSite.setBody("程序员的世界，其实很丰富!");
		crawlData4PortalSite.setMedia_type(2);
		crawlData4PortalSite.setTransmit_number(3);
		crawlData4PortalSite.setIs_new(0);
		List<CrawlData4PortalSite> list = new LinkedList<CrawlData4PortalSite>();
		list.add(crawlData4PortalSite);
		
		// esIndexManager.updateAndSaveIndex(searchConditionPojo, list,
		// newFieldsMap);
		
		esIndexManager.addBatchIndexByObj(indexName, typeName, list);

		System.out.println("执行完成!");
	}
}
