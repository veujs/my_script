import logging
import time
from django.core.paginator import Paginator
from fun_tools.utils import list_dict_duplicate_removal, compare_duplicate_result
from orders.models_backup import OrdersInfo, ExecutePlans, MissionInfo
from source.callback_handle import callback_post_data
from source.models import CrawlResults


logger = logging.getLogger(__file__)


def crawl_result_handle(res_data, plan_id):
    # 执行计划爬取结果处理
    mission_id = res_data.get('execute_plan_info').get('mission_id')
    params = res_data.get('execute_plan_info').get('params')
    # 预计执行时间
    execute_at = res_data.get('execute_plan_info').get('execute_at')
    # 实际执行时间
    reality_start_time = res_data.get('request_time')
    # 爬取结果
    crawl_result = res_data.get('crawl_result')
    # 爬虫开始时间
    spider_start_time = res_data.get('spider_start_time')
    # 爬虫运行结束时间
    spider_end_time = res_data.get('spider_end_time')
    # 爬虫运行时间
    spider_running_time = spider_end_time - spider_start_time
    # 实际执行时间与预计执行时间的差值
    sub_reality_execute = reality_start_time - execute_at
    # orders = OrdersInfo.objects(mission_id=mission_id, status=1)
    order_ids = MissionInfo.objects(mission_id=mission_id, status=1).first().order_ids

    if order_ids:
        try:
            for order_id in order_ids:
                # order_info = order.to_json()
                order = OrdersInfo.objects.filter(id=order_id)
                order_id = order.id
                callback = order.callback
                c_time = int(time.time())
                # 判断订单的频率是否是0 更新订单和任务状态
                change_once_status(order, params, c_time)
                # 过滤
                result = filter_keywords(order, crawl_result)
                # 执行结束时间
                end_time = int(time.time())
                # 执行结束时间和实际执行时间的差值
                sub_end_time_reality = end_time - reality_start_time
                # 执行结束时间与预计执行时间的差值
                sub_end_time_execute = end_time - execute_at
                # 保存数据
                if result:
                    print('Save a data info.......')

                    """对微博的爬取结果进行进一步过滤"""
                    result_, need_send = compare_duplicate_result(order, result)

                    save_data(mission_id, order_id, result, execute_at, reality_start_time, spider_running_time,
                              sub_reality_execute, end_time, sub_end_time_reality, sub_end_time_execute)

                    if need_send:
                        # # 处理有回调地址的订单
                        if callback:
                            data = {}
                            data['order_id'] = order_id
                            data['opinions'] = result_

                            print('Execution callback .......')
                            # async_handler_callback.delay(callback, data)
                            callback_post_data(callback, data)
                else:
                    print('Order_id: %s crawl result is None. This mission_id is  %s' % (order_id, mission_id))
                    pass
        except Exception as e:
            logger.exception(e)
            change_plan_status(plan_id, status=3)
            return {'result': 'Fail', 'msg': '数据提交失败,%s' % e}
        else:
            # 跟新任务计划状态
            change_plan_status(plan_id)
            return {'result': 'ok', 'msg': '数据提交成功'}
    else:

        change_plan_status(plan_id, status=3)
        return {'result': 'Fail', 'msg': '任务对应的订单不存在或已完成啦,任务单号: %s' % mission_id}


def pagination(data_s, offset, page):
    # 分页
    paginator = Paginator(data_s, offset)
    count = paginator.count
    pages_num = paginator.num_pages
    contacts = paginator.get_page(page)

    return count, pages_num, contacts


def filter_keywords(order, crawl_result):
    # 关键词过滤
    kw = order.params.get('kw', [])
    no_kw = order.params.get('no_kw', [])
    rate = order.params.get('rate', 0)
    result = []
    try:
        if kw:
            for item in crawl_result:
                if 'title' not in item:
                    raise Exception('爬虫数据中缺少 title 字段!')

                # 微信爬虫结果单独处理
                if order.spider_id == 2:
                    if 'keyword' not in item:
                        raise Exception('微信爬虫数据中缺少 keyword 字段!')
                    else:
                        for k in kw:
                            if k == item['keyword']:
                                result.append(item)
                else:
                    for k in kw:
                        title = item.get('title')
                        if k in title:
                            if no_kw:
                                for no_k in no_kw:
                                    if no_k in title:
                                        pass
                                    else:
                                        result.append(item)
                            else:
                                result.append(item)
            return list_dict_duplicate_removal(result)
    except Exception as e:
        print('Filter keywords error %s' % e)
        logger.exception(e)
    else:

        if rate == 0:
            return crawl_result
        else:
            return []


def change_once_status(order, params, c_time):
    # 更改一次性任务计划状态
    if order.params.get('rate') == 0:
        order.update(set__status=2, set__updated_at=c_time)
        MissionInfo.objects(params=params).update(set__status=2, set__updated_at=c_time)


def change_plan_status(plan_id, status=2):
    # 更改执行计划住状态
    try:
        ExecutePlans.objects(id=plan_id).update(set__status=status)
    except Exception as e:
        logger.exception(e)


def save_data(mission_id, order_id, result, execute_at, reality_start_time, spider_running_time,
              sub_reality_execute, end_time, sub_end_time_reality, sub_end_time_execute):
    # 保存数据到数据库
    save_result = CrawlResults(mission_id=mission_id, order_id=order_id, execute_at=execute_at,
                               end_time=end_time, result=result,
                               reality_start_time=reality_start_time,
                               spider_running_time=spider_running_time,
                               sub_reality_execute=sub_reality_execute,
                               sub_end_time_reality=sub_end_time_reality,
                               sub_end_time_execute=sub_end_time_execute
                               )
    save_result.save()
