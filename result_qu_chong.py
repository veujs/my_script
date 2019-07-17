import time
import pymongo
from functools import reduce
from itertools import groupby
from operator import itemgetter
import queue
import threading

client1 = pymongo.MongoClient(
    "mongodb://root:B99m&l8LY^%hg3vefpO9p)4kyF!tsvPD@dds-bp19d0ea34ec38941316-pub.mongodb.rds.aliyuncs.com:3717/")
client2 = pymongo.MongoClient(host='172.31.11.218', port=27017)

db1 = client1.SpiderTasks
db2 = client2.SpiderTasks
#
remote_orders_info = db2["orders_info"]

# 需要去重的集合
before_collection = db2["crawl_results"]
after_collection = db2["crawl_results_after"]
tmp_collection = db2["crawl_results_tmp"]

# ceshi 数据库
client3 = pymongo.MongoClient(host='172.16.10.40', port=27017)  # myubuntu
db3 = client3.SpiderTasks
test1_colection = db3["crawl_results"]
test2_colection = db3["crawl_results_after"]

# 定义去重后的数据

unique_id_map = {
    "1": "did",  # 豆瓣
    "2": "title",  # weixin
    "3": "wid",  # 微博
    "4": "title",  # 微博热搜
    "5": None,  # 微博话题   ##  没有？？？？
    "6": None,  # 百度贴吧   ##  没有？？？？
    "7": "title",  # 头条
    "8": "h_id",  # 虎扑
    "9": "tid",  # 图区
    "10": "title",  # 腾讯
    "11": "title",  # 凤凰
    "12": "title",  # 新浪新闻
    "13": "title",  # 搜狐新闻
    "14": "title",  # 网易新闻
    "15": None,  #
    "16": None,  #
    "17": None,  # 微博KOL验证
    "18": None,  # 百度贴吧名称验证
    "19": None,  # 图区是否删帖验证
    "20": None,  # douban小组关注人数验证   或者  "hd_id"?????
    "21": None  # 漏爬  豆瓣 微博  BUCHULI   !!!!
}





def translate_crawl_results(before_collection, after_collection, limit, skip):
    """skip方法在大量数据处理查询的时候下频率低下，不建议使用"""
    all_results = before_collection.find().limit(limit).skip(skip)

    rows = []
    ids = []
    for row in all_results:
        rows.append(row)
        ids.append(row['_id'])
    after_collection.insert_many(rows)


def translate_crawl_results2(before_collection, after_collection, limit, start_id):
    """修改使用skip方法---利用排序，截取的方法"""
    some_results = before_collection.find({"_id": {"$gte": start_id}}).limit(limit)
    after_collection.insert_many(some_results)


class dumplicate_crawl_reults():
    """
    对结果去重
    :param before_collection:
    :param after_collection:
    """

    def __init__(self, before_collection, after_collection, unique_id_map):
        self.before_collection = before_collection
        self.after_collection = after_collection
        self.unique_id_map = unique_id_map
        self.group_queue = queue.Queue()
        self.count_queue = queue.Queue()

    def aggregate_group(self):
        # 1、查询，分组
        # 分组  根据order_id分组
        group = {
            "$group": {
                "_id": "$order_id",
                'counts': {"$sum": 1},
                'results': {"$push": "$result"}
            }
        }
        #  分组
        order_id_group = self.before_collection.aggregate([group, ], allowDiskUse=True)
        """
        分组后结果  为查询集   通过for循环逐个访问
        {'_id': '', 'counts': 1, 'result': []}
        {'_id': '35a9b946-e784-41dd-ab70-fd0aadf289ad110', 'counts': 1, 'result': [[{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}]]}
        {'_id': '35a9b946-e784-41dd-ab70-fd0aadf289ad', 'counts': 2, 'result': [[{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}], [{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}]]}

        """
        return order_id_group

    def list_dict_duplicate_removal(self, list_dict):
        """
        列表中的字典元素去重
        """
        run_function = lambda x, y: x if y in x else x + [y]
        return reduce(run_function, [[], ] + list_dict)

    def filter_and_insert(self, results, spider_id, order_id, unique_id):
        """
        :param results:             待去重结果
        :param spider_id:           平台
        :param order_id:            订单id
        :param unique_id:           去重依据
        """
        current_time = int(time.time())
        if not unique_id:
            # new_one = {
            #     "order_id": order_id,
            #     "spider_id": spider_id,
            #     "created_at": int(time.time()),
            #     "updated_at": int(time.time()),
            #     "results": results
            # }
            # after_collection.insert(new_one)
            pass
        else:
            # 列表中只保留唯一的did
            union_results = []
            results.sort(key=itemgetter(unique_id))  # 需要先排序，然后才能groupby
            unique_id_group = groupby(results, itemgetter(unique_id))
            unique_id_results_list = [(key, list(group)) for key, group in unique_id_group]  # 耗时
            for unique_id_, unique_id_results in unique_id_results_list:
                if len(unique_id_results) > 1:
                    do_time = [one.get('do_time') for one in unique_id_results]
                    union_results.append(unique_id_results[do_time.index(max(do_time))])
                else:
                    union_results.append(unique_id_results[0])

            # 查询after_colection中  有无该订单的结合
            obj = self.after_collection.find_one({"order_id": order_id})
            if obj:
                try:
                    import operator
                    o_results = obj.get("results")
                    ascend_o_results = sorted(o_results, key=operator.itemgetter("create_time"))

                    # 删除三个月前发布的数据
                    create_time_list = [o.get('create_time') for o in ascend_o_results]

                    tmp = 0
                    for as_i in create_time_list:
                        if as_i > current_time - 3 * 30 * 24 * 60 * 60:
                            tmp = create_time_list.index(as_i)
                            break
                    o_results = ascend_o_results[tmp:]
                    o_unique_id_list = [o_result.get(unique_id) for o_result in o_results]  # weibo 唯一标识列表
                    update_flag = 0
                    for union_result in union_results:
                        union_unique_id = union_result.get(unique_id)
                        if union_unique_id not in o_unique_id_list:

                            # print("新添加一个unique_id的结果", union_unique_id)
                            "新添加一个unique_id的结果"
                            o_results.append(union_result)
                            update_flag += 1
                        else:
                            # 覆盖原有内容
                            o_unique_id_list = [o.get(unique_id) for o in o_results]  # weibo 唯一标识列表
                            o_unique_id_index = o_unique_id_list.index(union_unique_id)  # 获取对应的索引
                            if union_result.get("do_time") > o_results[o_unique_id_index].get("do_time"):
                                o_results[o_unique_id_index] = union_result
                                update_flag += 1
                    if update_flag > 0:
                        self.after_collection.update({"_id": obj.get('_id')},
                                                     {"$set": {"results": o_results, "updated_at": int(time.time())}})
                except Exception as e:
                    print(spider_id, unique_id, obj.get('order_id'))
                    print(e)
            else:
                new_one = {
                    "order_id": order_id,
                    "spider_id": spider_id,
                    "created_at": int(time.time()),
                    "updated_at": int(time.time()),
                    "results": union_results
                }
                self.after_collection.insert(new_one)

    def execute_merge(self):
        """
        执行合并（单个group）
        :param one_order_group:
        :return:
        """
        while True:
            if not self.group_queue.empty():
                try:
                    one_order_group = self.group_queue.get(block=False)
                except Exception as e:
                    print(e)
                    break
                try:
                    if one_order_group:
                        # 1 准备  filter_and_insert 所需参数
                        order = remote_orders_info.find_one({'_id': one_order_group.get('_id')})
                        if not order:
                            # invalid_order = one_order_group.get('_id')
                            # return invalid_order
                            self.group_queue.task_done()
                            continue
                        spider_id = order.get("spider_id")
                        order_id = one_order_group.get("_id")

                        results_list = one_order_group.get("results")  # list[list[]]

                        # list[list[]] ---> list[]
                        results = []
                        for result_list in results_list:
                            results += result_list
                        results = self.list_dict_duplicate_removal(results)  # 列表字典去重
                        # print(results)
                        unique_id = self.unique_id_map.get(str(spider_id))

                        # 2 执行去重迁移
                        # 根据选择的唯一标识去重，
                        self.filter_and_insert(results=results, spider_id=spider_id, order_id=order_id, unique_id=unique_id)
                        # print("完成1组........................................")
                        self.count_queue.put(one_order_group)
                        print("11", self.group_queue.qsize())
                        print("22", self.count_queue.qsize())
                        self.group_queue.task_done()

                except Exception as e:
                    print(e)
                    break
            else:
                # self.group_queue.task_done()
                # print("0000000")
                break


    def run(self):
        # invalid_order_ids = []
        thread_list = []
        group_count = 0
        # 1 获取所有group
        order_id_group = self.aggregate_group()

        # 2 把所有group压入队列
        for one_order_group in order_id_group:
            self.group_queue.put(one_order_group)
            group_count += 1

        # print(self.group_queue.qsize())
        print("待完成队列大小", group_count)

        # 3 开启线程
        for i in range(2000):
            t = threading.Thread(target=self.execute_merge)
            t.setDaemon(True)
            t.start()
            # thread_list.append(t)

        self.group_queue.join()

        print("完成队列大小：", self.count_queue.qsize())
        # print("----------------------------------------------------")
        # print("无效订单的数量： {}".format(len(invalid_order_ids)))


def clear_collection(collection):
    """
    清空集合
    :param collection: 待清空的集合
    """
    collection.drop()


if __name__ == '__main__':

    du_class = dumplicate_crawl_reults(tmp_collection, after_collection, unique_id_map)  # tmp_collection    ----> after_collection


    # 1993 1502
    # for i in range(3986, 3987):
    #     print("剩余: {}".format(19931502 - (i - 1) * 5000))
    for i in range(4025, 5917):
        print("剩余: {}".format(29581502 - (i - 1) * 5000))
        start_time1 = time.time()
        # # 上10000个的最后一个
        if i == 1:
            start_one = before_collection.find().sort('_id', pymongo.ASCENDING)[0]
        else:
            # start_one = tmp_collection.find().sort('_id', pymongo.DESCENDING)[0]
            start_one = before_collection.find().sort('_id', pymongo.ASCENDING)[(i - 1) * 5000]
        start_id = start_one.get("_id")
        clear_collection(tmp_collection)  # clear tmp_collection
        end_time = time.time()
        ser_time = int(end_time - start_time1)
        print("搜索起始位置用时： {} s".format(ser_time))

        start_time2 = time.time()
        translate_crawl_results2(before_collection, tmp_collection, 5000,
                                 start_id)  # before_collection ----> tmp_collection
        end_time = time.time()
        trans_time = int(end_time - start_time2)
        print("转移用时： {} s".format(trans_time))

        # print("222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222")

        # some_results = before_collection.find({"_id": {"$gte": start_id}}).limit(10000)
        start_time3 = time.time()
        du_class.run()  # tmp_collection    ----> after_collection
        end_time = time.time()
        du_time = int(end_time - start_time3)
        print("去重用时： {} s".format(du_time))

        # print("111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        end_time = time.time()
        sum = int(end_time - start_time1)
        speed = int(5000 / sum)
        print("速度: {} 个/s".format(speed))
        print("还需要: {} s".format(int((29581502 - i * 5000) / speed)))
        print("第 {} 次 循环结束".format(i))
        print("")
        print("")

    clear_collection(tmp_collection)  # clear tmp_collection

    # translate_crawl_results(before_collection, tmp_collection, 2, 1)  # before_collection ----> tmp_collection



