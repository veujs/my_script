import time
import pymongo
from functools import reduce
from itertools import groupby
from operator import itemgetter


client1 = pymongo.MongoClient("mongodb://root:B99m&l8LY^%hg3vefpO9p)4kyF!tsvPD@dds-bp19d0ea34ec38941316-pub.mongodb.rds.aliyuncs.com:3717/")
client2 = pymongo.MongoClient(host='192.168.11.4', port=27017)

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

fm2 = {
    "order_id": "",
    "spider_id": 1,
    "created_at": 12345678,
    "updated_at": 12345668,
    "results": [1, 2, 3]  # 持续填充， 限制（保留20条），暂不设置
}


unique_id_map = {
    "1": "did",         # 豆瓣
    "2": "title",      # weixin
    "3": "wid",         # 微博
    "4": "title",       # 微博热搜
    "5": None,          # 微博话题   ##  没有？？？？
    "6": None,          # 百度贴吧   ##  没有？？？？
    "7": "title",       # 头条
    "8": "h_id",        # 虎扑
    "9": "tid",         # 图区
    "10": "title",      # 腾讯
    "11": "title",      # 凤凰
    "12": "title",      # 新浪新闻
    "13": "title",      # 搜狐新闻
    "14": "title",      # 网易新闻
    "15": None,         #
    "16": None,         #
    "17": None,       # 微博KOL验证
    "18": None,         # 百度贴吧名称验证
    "19": None,        # 图区是否删帖验证
    "20": None,       # douban小组关注人数验证   或者  "hd_id"?????
    "21": None        # 漏爬  豆瓣 微博  BUCHULI   !!!!
}


def filter_and_insert(after_collection, results, spider_id, order_id, unique_id):
    """
    :param after_collection:    数据转存集合
    :param results:             待去重结果
    :param spider_id:           平台
    :param order_id:            订单id
    :param unique_id:           去重依据
    """
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
        obj = after_collection.find_one({"order_id": order_id})
        if obj:
            try:
                o_results = obj.get("results")
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
                    after_collection.update({"_id": obj.get('_id')},
                                            {"$set": {"results": o_results, "updated_at": int(time.time())}})
            except Exception as e:
                print(spider_id, unique_id)
                print(e)
        else:
            new_one = {
                "order_id": order_id,
                "spider_id": spider_id,
                "created_at": int(time.time()),
                "updated_at": int(time.time()),
                "results": union_results
            }
            after_collection.insert(new_one)


def list_dict_duplicate_removal(data_list):
    # 列表中的字典元素去重
    run_function = lambda x, y: x if y in x else x + [y]
    return reduce(run_function, [[], ] + data_list)


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


def dumplicate_crawl_reults(before_collection, after_collection):
    """
    对结果去重
    :param before_collection:
    :param after_collection:
    """
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
    order_id_group = before_collection.aggregate([group, ], allowDiskUse=True)
    """
    分组后结果  为查询集   通过for循环逐个访问
    {'_id': '', 'counts': 1, 'result': []}
    {'_id': '35a9b946-e784-41dd-ab70-fd0aadf289ad110', 'counts': 1, 'result': [[{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}]]}
    {'_id': '35a9b946-e784-41dd-ab70-fd0aadf289ad', 'counts': 2, 'result': [[{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}], [{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}]]}

    """
    invalid_order_ids = []
    # 2、合并
    for one_order_group in order_id_group:
        order = remote_orders_info.find_one({'_id': one_order_group.get('_id')})
        if not order:
            invalid_order_ids.append(one_order_group.get('_id'))
            # print("order_id: {} 已经删除.....".format(one_order_group.get('_id')))
            continue
        spider_id = order.get("spider_id")
        order_id = one_order_group.get("_id")

        start_time = time.time()
        # print("spider_id: {}, order_id: {}  counts: {}  处理中...................................".format(spider_id, one_order_group.get('_id'), one_order_group.get('counts')))

        results_list = one_order_group.get("results")  # list[list[]]

        # list[list[]] ---> list[]
        results = []
        for result_list in results_list:
            results += result_list
        results = list_dict_duplicate_removal(results)  # 列表字典去重
        # print(results)
        unique_id = unique_id_map.get(str(spider_id))

        # 根据选择的唯一标识去重，
        filter_and_insert(after_collection=after_collection, results=results, spider_id=spider_id, order_id=order_id,
                          unique_id=unique_id)

        end_time = time.time()
        # print("spider_id: {}, order_id: {}, counts: {}  耗时......................................{}毫秒"
        #       .format(spider_id, one_order_group.get('_id'), one_order_group.get('counts'),
        #               int((end_time - start_time)*1000)))
    print("无效订单的数量： {}".format(len(invalid_order_ids)))


def clear_collection(collection):
    """
    清空集合
    :param collection: 待清空的集合
    """
    collection.drop()


if __name__ == '__main__':

    # 1993 1502
    for i in range(3347, 3987):
        print("剩余: {}".format(19931502 - (i - 1) * 5000))

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
        translate_crawl_results2(before_collection, tmp_collection, 5000, start_id)  # before_collection ----> tmp_collection
        end_time = time.time()
        trans_time = int(end_time - start_time2)
        print("转移用时： {} s".format(trans_time))

        # some_results = before_collection.find({"_id": {"$gte": start_id}}).limit(10000)
        start_time3 = time.time()
        dumplicate_crawl_reults(tmp_collection, after_collection)                      # tmp_collection    ----> after_collection
        end_time = time.time()
        du_time = int(end_time - start_time3)
        print("去重用时： {} s".format(du_time))

        end_time = time.time()
        sum = int(end_time - start_time1)
        speed = int(5000 / sum)
        print("速度: {} 个/s".format(speed))
        print("还需要: {} s".format(int((19931502 - i * 5000) / speed)))
        print("第 {} 次 循环结束".format(i))
        print("")
        print("")

    clear_collection(tmp_collection)  # clear tmp_collection

    # translate_crawl_results(before_collection, tmp_collection, 2, 1)  # before_collection ----> tmp_collection
    # dumplicate_crawl_reults(tmp_collection, after_collection)    # tmp_collection    ----> after_collection
    # clear_collection(tmp_collection)  #  clear tmp_collection








# def dumplicate_crawl_reults(before_collection, after_collection):
#     # 1、查询，分组
#     # 分组  根据order_id分组ed
#     group = {
#         "$group": {
#             "_id": "$order_id",
#             'counts': {"$sum": 1},
#             'results': {"$push": "$result"}
#         }
#     }
#     #  分组
#     order_id_group = before_collection.aggregate([group, ], allowDiskUse=True)
#     """
#     分组后结果  为查询集   通过for循环逐个访问
#     {'_id': '', 'counts': 1, 'result': []}
#     {'_id': '35a9b946-e784-41dd-ab70-fd0aadf289ad110', 'counts': 1, 'result': [[{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}]]}
#     {'_id': '35a9b946-e784-41dd-ab70-fd0aadf289ad', 'counts': 2, 'result': [[{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}], [{'name': '胡彦斌', 'wid': 'HxOG8b3pY', 'woid': 1802742227, 'portrait': 'https://tvax4.sinaimg.cn/crop.0.0.1080.1080.180/6b73a9d3ly8fxktnokdksj20u00u041d.jpg', 'href': 'http://weibo.com/1802742227/HxOG8b3pY', 'title': '1111明晚就是创造2222营总决赛了，我们B班有十位同学即将在明晚冲刺成团，@创造营2019-何洛洛@创造营2019-夏之光@创造营2019-张颜齐@创造营2019-任豪@创造营2019-李鑫一@创造营2019-赵磊@创造营2019-赵泽帆@创造营2019-李昀锐@创造营2019-吴季峰@创造营2019-秦天为你们加油打气！也祝福我们所有B班...全文', 'attitudes_count': 12622, 'comments_count': 2752, 'reposts_count': 8968, 'create_time': 1559836800, 'attribute': 1, 'image_urls': [{'image_small_url': 'https://wx2.sinaimg.cn/orj360/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg', 'image_large_url': 'https://wx2.sinaimg.cn/large/6b73a9d3ly1g3spnjpe6aj20ku112aht.jpg'}], 'text_url': '', 'text_img': '', 'movie_urls': '', 'movie_img': '', 'do_time': 1561598504, 'retweeted': []}]]}
#
#     """
#     # 2、合并
#     for one_order_group in order_id_group:
#         order = remote_orders_info.find_one({'_id': one_order_group.get('_id')})
#         if not order:
#             print("order_id: {} 已经删除.....".format(one_order_group.get('_id')))
#             continue
#         spider_id = order.get("spider_id")
#         order_id = one_order_group.get("_id")
#
#         start_time = time.time()
#         print("spider_id: {}, order_id: {}  counts: {}  处理中...................................".format(spider_id, one_order_group.get('_id'), one_order_group.get('counts')))
#
#         results_list = one_order_group.get("results")  # list[list[]]
#
#         # list[list[]] ---> list[]
#         results = []
#         for result_list in results_list:
#             results += result_list
#         results = list_dict_duplicate_removal(results) # 列表字典去重
#         # print(results)
#
#         if spider_id == 3:  # weibo
#             # 列表中只保留唯一的wid
#             union_results = []
#             results.sort(key=itemgetter('wid'))  # 需要先排序，然后才能groupby
#             wid_group = groupby(results, itemgetter('wid'))
#             wid_results_list = [(key, list(group)) for key, group in wid_group]  # 耗时
#             for wid, wid_results in wid_results_list:
#                 print(wid, wid_results)
#                 if len(wid_results) > 1:
#                     do_time = [one.get('do_time') for one in wid_results]
#                     union_results.append(wid_results[do_time.index(max(do_time))])
#                 else:
#                     union_results.append(wid_results[0])
#
#             # 查询after_colection中  有无该订单的结合
#             obj = after_collection.find_one({"order_id": order_id})
#             if obj:
#                 o_results = obj.get("results")
#                 wid_list = [o_result.get('wid') for o_result in o_results]  # weibo 唯一标识列表
#                 for union_result in union_results:
#                     wid = union_result.get('wid')
#                     if wid not in wid_list:
#                         print("新添加一个wid的结果", wid)
#                         "新添加一个wid的结果"
#                         o_results.append(union_result)
#                     else:
#                         # 覆盖原有内容
#                         wid_list = [o.get('wid') for o in o_results]  # weibo 唯一标识列表
#                         o_wid_index = wid_list.index(wid)  # 获取对应的索引
#                         if union_result.get("do_time") > o_results[o_wid_index].get("do_time"):
#                             o_results[o_wid_index] = union_result
#                 after_collection.update({"_id": obj.get('_id')}, {"$set": {"results": o_results, "updated_at": int(time.time())}})
#             else:
#                 new_one = {
#                     "order_id": order_id,
#                     "spider_id": spider_id,
#                     "created_at": int(time.time()),
#                     "updated_at": int(time.time()),
#                     "results": union_results
#                 }
#                 after_collection.insert(new_one)
#
#         end_time = time.time()
#         print("spider_id: {}, order_id: {}, counts: {}  耗时......................................{}毫秒"
#               .format(spider_id, one_order_group.get('_id'), one_order_group.get('counts'),
#                       int((end_time - start_time)*1000)))
#     pass





