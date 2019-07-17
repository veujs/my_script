
import pymongo
import time
import math

MONGODB_HOSTS = [
    'dds-bp19d0ea34ec38941316-pub.mongodb.rds.aliyuncs.com:3717',
    'dds-bp19d0ea34ec38942764-pub.mongodb.rds.aliyuncs.com:3717'
]

client1 = pymongo.MongoClient("mongodb://root:B99m&l8LY^%hg3vefpO9p)4kyF!tsvPD@dds-bp19d0ea34ec38941316-pub.mongodb.rds.aliyuncs.com:3717/")
client2 = pymongo.MongoClient(host='192.168.11.4', port=27017)
client3 = pymongo.MongoClient(host='127.0.0.1', port=27017)
client4 = pymongo.MongoClient(host='192.168.11.2', port=27017)

db1 = client1.SpiderTasks

db2 = client2.SpiderTasks

db3 = client3.SpiderTasks

db4 = client4.SpiderTasks


def translate_orders():

    start_at = int(time.time() * 1000)
    all_result = db1['orders_info'].find({})
    total = all_result.count()

    if total == 0:
        print("转移完成")
        exit()

    count = 0
    print("剩余: {}".format(total))
    # result = all_result.limit(num)

    rows = []
    ids = []

    for row in all_result:
        count += 1
        rows.append(row)
        ids.append(row['_id'])

    query_end_at = int(time.time() * 1000)
    print("查询遍历用时: {} 毫秒".format(query_end_at - start_at))

    insert_start_at = int(time.time() * 1000)
    db2['orders_info'].insert_many(rows)
    insert_end_at = int(time.time() * 1000)
    print("插入用时: {} 毫秒".format(insert_end_at - insert_start_at))

    end_at = int(time.time() * 1000)
    cost = end_at - start_at

    speed = count / cost * 1000

    left_time = (total - count) / speed

    day = 0
    hour = 0
    minute = 0

    if left_time > 86400:
        day = math.floor(left_time / 86400)
        left_time = left_time - (day * 86400)

    if left_time > 3600:
        hour = math.floor(left_time / 3600)
        left_time = left_time - (hour * 3600)

    if left_time > 60:
        minute = math.floor(left_time / 60)
        left_time = left_time - (minute * 60)

    print("速度: {}, 预计剩余 {} 天 {} 小时 {} 分钟 {} 秒".format(speed, day, hour, minute, math.floor(left_time)))

    print("")
    print("")


def update_orders():

    all_result = db4['orders_info'].find()
    total = all_result.count()
    print("剩余: {}".format(total))

    update_start_at = int(time.time() * 1000)
    db4['orders_info'].update_many({'status': {'$in': [1]}}, {'$set': {'status': 0}})
    update_end_at = int(time.time() * 1000)
    print("更新用时: {} 毫秒".format(update_end_at - update_start_at))

    print("")
    print("")


def translate_spiders():

    start_at = int(time.time() * 1000)
    all_result = db4['spider_info'].find()
    total = all_result.count()

    if total == 0:
        print("转移完成")
        exit()

    count = 0
    print("剩余: {}".format(total))
    # result = all_result.limit(num)

    rows = []
    ids = []

    for row in all_result:
        count += 1
        rows.append(row)
        ids.append(row['_id'])

    query_end_at = int(time.time() * 1000)
    print("查询遍历用时: {} 毫秒".format(query_end_at - start_at))

    insert_start_at = int(time.time() * 1000)
    db3['spider_info'].insert_many(rows)
    insert_end_at = int(time.time() * 1000)
    print("插入用时: {} 毫秒".format(insert_end_at - insert_start_at))

    end_at = int(time.time() * 1000)
    cost = end_at - start_at

    speed = count / cost * 1000

    left_time = (total - count) / speed

    day = 0
    hour = 0
    minute = 0

    if left_time > 86400:
        day = math.floor(left_time / 86400)
        left_time = left_time - (day * 86400)

    if left_time > 3600:
        hour = math.floor(left_time / 3600)
        left_time = left_time - (hour * 3600)

    if left_time > 60:
        minute = math.floor(left_time / 60)
        left_time = left_time - (minute * 60)

    print("速度: {}, 预计剩余 {} 天 {} 小时 {} 分钟 {} 秒".format(speed, day, hour, minute, math.floor(left_time)))

    print("")
    print("")


def translate_execute_plans():

    start_at = int(time.time() * 1000)
    all_result = db1['execute_plans'].find()
    total = all_result.count()

    if total == 0:
        print("转移完成")
        exit()

    count = 0
    print("剩余: {}".format(total))
    # result = all_result.limit(num)

    rows = []
    ids = []

    for row in all_result:
        count += 1
        rows.append(row)
        ids.append(row['_id'])

    query_end_at = int(time.time() * 1000)
    print("查询遍历用时: {} 毫秒".format(query_end_at - start_at))

    insert_start_at = int(time.time() * 1000)
    db3['execute_plans'].insert_many(rows)
    insert_end_at = int(time.time() * 1000)
    print("插入用时: {} 毫秒".format(insert_end_at - insert_start_at))

    end_at = int(time.time() * 1000)
    cost = end_at - start_at

    speed = count / cost * 1000

    left_time = (total - count) / speed

    day = 0
    hour = 0
    minute = 0

    if left_time > 86400:
        day = math.floor(left_time / 86400)
        left_time = left_time - (day * 86400)

    if left_time > 3600:
        hour = math.floor(left_time / 3600)
        left_time = left_time - (hour * 3600)

    if left_time > 60:
        minute = math.floor(left_time / 60)
        left_time = left_time - (minute * 60)

    print("速度: {}, 预计剩余 {} 天 {} 小时 {} 分钟 {} 秒".format(speed, day, hour, minute, math.floor(left_time)))

    print("")
    print("")

if __name__ == "__main__":

    # while True:
    # translate_orders()
    # update_orders()
    # translate_spiders()
    translate_execute_plans()
