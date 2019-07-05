import time

from mongoengine import Document, fields, QuerySet


class TimeDocument(object):
    # 创建时间时间戳
    created_at = fields.IntField(required=True, verbose_name='创建时间', default=time.time)
    # 更新时间时间戳
    updated_at = fields.IntField(required=True, verbose_name='更新时间', default=time.time)


class DefinedQuerySet(QuerySet):
    '''
    自定义查询
    '''

    def order_get_and_update_status(self):
        # 获取未分配订单更改其状态
        objects = self.filter(status=0)
        for obj in objects:
            obj.update(set__status=5)
        return objects

    def mission_get_and_update_status(self, execute_at):
        # 获取该要制定执行计划的状态任务更改其状态
        objects = self.filter(execute_plan_end_at__lte=execute_at, status=1)

        for obj in objects:
            obj.update(set__status=0)
        return objects


ORDER_STATUS = (
    (0, '未分配'), (1, '订单执行中'), (2, '订单已完成'), (3, '订单执行失败'), (4, '订单已过期'),(5,'分配中')
)


class OrdersInfo(Document, TimeDocument):
    # 订单编号
    id = fields.StringField(required=True, primary_key=True, verbose_name='订单编号', help_text='订单编号')
    status = fields.IntField(required=True, choices=ORDER_STATUS, default=0, verbose_name='状态', help_text='状态')
    # 爬虫类型
    spider_id = fields.IntField(required=True, verbose_name='爬虫类型', help_text='爬虫类型')
    # 任务编号
    mission_id = fields.StringField(verbose_name='任务编号', help_text='任务编号', null=True, default=None)
    # 附加参数 json
    params = fields.DictField(required=True, verbose_name='附加参数', help_text='附加参数')
    # 回调地址
    callback = fields.URLField(verbose_name='回调地址', help_text='回调地址', null=True)
    meta = {
        'indexes': ['status', 'params', 'spider_id', 'created_at'],
        'queryset_class': DefinedQuerySet
    }

    class Meta:
        Verbose_name = '订单信息'

    def __str__(self):
        return self.id


MISSION_STATUS = (
    (0, '制定执行计划中'), (1, '任务执行中'), (2, '任务执行完成'), (3, '任务执行失败'),
)


class MissionInfo(Document, TimeDocument):
    mission_id = fields.StringField(verbose_name='任务编号', help_text='任务编号')
    status = fields.IntField(required=True, choices=MISSION_STATUS, default=1, verbose_name='状态', help_text='状态')
    # 爬虫类型
    spider_id = fields.IntField(required=True, verbose_name='爬虫类型', help_text='爬虫类型')
    # 附加参数
    params = fields.DictField(required=True, verbose_name='附加参数', help_text='附加参数')
    # 执行计划最后执行时间戳
    execute_plan_end_at = fields.IntField(required=True, help_text="最后执行时间戳", verbose_name='最后执行时间戳')
    order_ids = fields.ListField(help_text="订单列表", verbose_name='订单列表', null=True, default=None)
    meta = {
        'indexes': ['#spider_id', ('execute_plan_end_at', 'spider_id'), '#order_ids', 'created_at'],
        'queryset_class': DefinedQuerySet
    }

    class Meta:
        Verbose_name = '任务信息'

    def __str__(self):
        return self.mission_id


PLANS_STATUS = (
    (0, '预执行'), (1, '未执行'), (2, '执行成功'), (3, '执行失败'), (4, '执行计划过期'))


class ExecutePlans(Document, TimeDocument):
    status = fields.IntField(required=True, choices=PLANS_STATUS, default=1)
    # 爬虫的类型
    spider_id = fields.IntField(required=True, verbose_name='爬虫类型', help_text='爬虫类型')
    # 爬虫名
    spider_name = fields.StringField(required=True, verbose_name='爬虫名', help_text='爬虫名')
    # 任务编号
    mission_id = fields.StringField(required=True, verbose_name='任务编号', help_text='任务编号')
    # 执行时间时间戳
    execute_at = fields.IntField(required=True, help_text="执行计划时间", verbose_name='执行计划时间')
    # 附加参数
    params = fields.DictField(required=True, verbose_name='附加参数', help_text='附加参数')

    meta = {
        'indexes': ['#execute_at', '#status', ('spider_id', 'execute_at')],
        'queryset_class': DefinedQuerySet
    }

    class Meta:
        Verbose_name = '执行计划信息'

    def __str__(self):
        return self.mission_id
