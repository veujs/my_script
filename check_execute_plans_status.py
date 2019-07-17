import pymongo
import time

# MONGODB_HOSTS = [
#     'dds-bp19d0ea34ec38941316-pub.mongodb.rds.aliyuncs.com:3717',
#     'dds-bp19d0ea34ec38942764-pub.mongodb.rds.aliyuncs.com:3717'
# ]

client1 = pymongo.MongoClient("mongodb://root:B99m&l8LY^%hg3vefpO9p)4kyF!tsvPD@dds-bp19d0ea34ec38941316-pub.mongodb.rds.aliyuncs.com:3717/")
# 声明数据库 线上数据库
db1 = client1.SpiderTasks

# 声明集合 任务  计划
mission_info = db1["mission_info"]
execute_plans = db1["execute_plans"]

if __name__ == '__main__':

    all_start_time = int(time.time())
    c_time = int(time.time())
    all_missions = mission_info.find({"status": 1})
    print(all_missions.count())

    # 针对spider_id  3 进行分析
    # 1 获取正在运行中的任务
    weibo_missions = mission_info.find({"spider_id": 3, "status": 1})

    if not weibo_missions:
        print("当前微博没有执行中的任务")
    else:
        print("")
        count = 0
        for m in weibo_missions:
            count += 1
            print(count)
            # print(m.get("mission_id"))
            # 查询该任务  1 小时内的计划
            print("mission_id: %s" % m.get("mission_id"))
            rate = m.get("params").get("rate")
            start_time = c_time - 60 * 60 - int(rate/1000)
            end_time = c_time - int(rate/1000)
            print("start_time: %s" % start_time)
            print("end_time: %s" % end_time)
            one_hour_plans = execute_plans.find({"mission_id": m.get("mission_id"),
                                                 "execute_at": {"$lt": end_time, "$gte": start_time}
                                                 })
            one_hour_plans_list = [p for p in one_hour_plans]
            print(one_hour_plans_list)
            # break
            # one_hour_plans_of_2 = execute_plans.find({"mission_id": m.get("mission_id"),
            #                                           "execute_at": {"$lt": end_time, "$gte": start_time} ,
            #                                           "status": 2
            #                                           })
            # print(type(one_hour_plans))
            # print(one_hour_plans.count())
            # plans_of_0 = []
            # plans_of_1 = []
            # plans_of_2 = []
            # plans_of_3 = []
            #
            # if one_hour_plans_of_2 != one_hour_plans.count():
            #     # pass
            #     for p in one_hour_plans:
            #         # if
            #         print(p.get('_id'))
            # break
    all_end_time = int(time.time())
    print("总用时为：{} s".format(all_end_time - all_start_time))
    print(weibo_missions.count())


    # 2 计划生成的策略是生成每小时之内的订单
