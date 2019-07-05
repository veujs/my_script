


import time
import mongoengine
from mongoengine import Document, fields, QuerySet
MONGODB_HOSTS = '127.0.0.1'

mongoengine.connect('SpiderTasks', host=MONGODB_HOSTS, connect=False, maxPoolSize=200, )

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
        objects = self.filter(status=0)[0:500]
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
    # # 爬虫类型
    # spider_id = fields.IntField(required=True, verbose_name='爬虫类型', help_text='爬虫类型')
    # # 任务编号
    # mission_id = fields.StringField(verbose_name='任务编号', help_text='任务编号', null=True, default=None)
    # # 附加参数 json
    # params = fields.DictField(required=True, verbose_name='附加参数', help_text='附加参数')
    # # 回调地址
    # callback = fields.URLField(verbose_name='回调地址', help_text='回调地址', null=True)

    def change(self,name):
        self.meta['collection'] = name
    meta = {
        'collection': 'WWZZPP',
        # 'indexes': ['status', 'params', 'spider_id'],
        # 'queryset_class': DefinedQuerySet
    }

    class Meta:
        Verbose_name = '订单信息'

    def __str__(self):
        return self.id


from django.db import models

class Base(Document):
    class Meta:
        abstract = True

    @classmethod
    def setDb_table(Class, tableName):
        class Meta:
            db_table = tableName

        attrs = {
            '__module__': Class.__module__,
            'Meta': Meta
        }
        return type(tableName, (Class,), attrs)
    id = fields.StringField(required=True, primary_key=True, verbose_name='订单编号', help_text='订单编号')
    status = fields.IntField(required=True, choices=ORDER_STATUS, default=0, verbose_name='状态', help_text='状态')



if __name__ == '__main__':
    new_N = Base.setDb_table("WZP")
    pass