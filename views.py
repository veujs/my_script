import time
import uuid
from rest_framework import status
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.settings import api_settings
from rest_framework.views import APIView
from mongoengine import Q
from rest_framework_mongoengine.viewsets import ModelViewSet
from orders.models_backup import OrdersInfo, MissionInfo, ExecutePlans
from orders.serializers import OrdersCreateSerializer, OrdersDetailSerializer, OrdersUpdateSerializer
import logging
from .mission_jishu import miss
logger = logging.getLogger(__name__)


class OrdersPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    page_size_query_description = ('每页返回的结果数.')
    page_query_param = "page"
    page_query_description = ('分页结果集中的页码.')
    max_page_size = 100


class OrdersViewSet(ModelViewSet):
    lookup_field = 'id'
    pagination_class = OrdersPagination
    queryset = OrdersInfo.objects.all()

    def get_serializer_class(self):
        if self.action == "create":
            return OrdersCreateSerializer
        elif self.action == "retrieve":
            return OrdersDetailSerializer
        elif self.action == "partial_update":
            return OrdersUpdateSerializer
        return OrdersDetailSerializer

    def create(self, request, *args, **kwargs):
        """新建任务订单"""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # 创建时间   # id  使用随机uuid
        id = str(uuid.uuid4())
        serializer.save(id=id)
        headers = self.get_success_headers(serializer.data)
        data = {'result': 'ok', 'order_id': id}
        return Response(data, status=status.HTTP_201_CREATED, headers=headers)

    def destroy(self, request, *args, **kwargs):
        '''# 删除任务订单'''
        # 查询订单实例
        instance = self.get_object()
        updated_at = int(time.time())

        if instance:
            order_id = instance.id
            mission_id = instance.mission_id
            # 修改任务
            if mission_id:
                missions = MissionInfo.objects(id=mission_id)

                if missions.count() != 0:
                    for m in missions:
                        m_order_ids = m.order_ids
                        if m_order_ids:
                            if order_id in m_order_ids:
                                m_order_ids.remove(order_id)
                                if len(m_order_ids) == 0:
                                    m.delete()
                                    ExecutePlans.objects(mission_id=mission_id).delete()
                                else:
                                    m.update(set__order_ids=m_order_ids, set__updated_at=updated_at)
                        else:
                            m.delete()
                            ExecutePlans.objects(mission_id=mission_id).delete()

            # 订单删除
            instance.delete()
            data = {'result': 'ok', 'msg': '任务订单删除成功'}
            return Response(data=data, status=status.HTTP_204_NO_CONTENT)
        
    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        updated_at = int(time.time())
        if getattr(instance, '_prefetched_objects_cache', None):
            instance._prefetched_objects_cache = {}
        data = request.data
        try:
            serializer = self.get_serializer(instance, data=data, partial=partial)
            serializer.is_valid(raise_exception=True)
            # 更新回调地址只需更新订单
            # 更新params 需要
            target = instance.params.get('target')
            old_kw = instance.params.get('kw')
            old_rate = instance.params.get('rate')
            old_no_kw = instance.params.get('no_kw')
            old_callback = instance.callback
            
            params = data.pop('params', False)
            new_callback = data.pop('callback', None)
            new_kw = params.get('kw')
            new_rate = params.get('rate')
            new_no_kw = params.get('no_kw')
            
            if target != params.get('target'):
                data = {'result': 'ok', 'msg': '系统不支持修改订单 target 参数'}
                return Response(data, status=status.HTTP_400_BAD_REQUEST)

            if new_rate != old_rate:
                status = 0
            else:
                status = instance.status
                
            if self.isRemovekeyword(new_kw, old_kw) or self.isRemovekeyword(new_no_kw, old_no_kw) \
                    or new_rate != old_rate or old_callback != new_callback or old_no_kw != new_no_kw:
                instance.update(set__mission_id=None, set__params__kw=new_kw, set__params__no_kw=new_no_kw,
                                set__callback=new_callback, set__params__rate=new_rate, set__updated_at=updated_at,
                                set__status=status)
            else:
                instance.update(set__updated_at=updated_at)
        except Exception as e:
            logger.exception(e)
            data = {'result': 'Fail', 'msg': '订单参数修改失败,%s' % e}
            return Response(data, status=status.HTTP_400_BAD_REQUEST)
        else:
            data = {'result': 'ok', 'msg': '订单参数修改成功'}
            return Response(data, status=status.HTTP_200_OK)

    def isRemovekeyword(self, newKeywords, oldKeywords):
        if len(newKeywords) == len(oldKeywords):
            for k in oldKeywords:
                if k not in newKeywords:
                    return True
            return False
        else:
            return True

    def partial_update(self, request, *args, **kwargs):
        kwargs['partial'] = True
        return self.update(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        '''所有任务订单详情'''
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def get_success_headers(self, data):
        try:
            return {'Location': str(data[api_settings.URL_FIELD_NAME])}
        except (TypeError, KeyError):
            return {}

    def retrieve(self, request, *args, **kwargs):
        '''# 单个任务订单详情'''
        instance = self.get_object()
        print(instance)
        serializer = self.get_serializer(instance)
        return Response(serializer.data)


class Update_Orders_Term_of_Validity(APIView):

    def post(self, request, *args, **kwargs):
        '''
        心跳 更新订单有效期
        :return:
        '''
        res_data = request.data
        orders_id = res_data.get('orders_id')
        orders_num = len(orders_id)
        invalid_orders = []
        if orders_num > 300:
            data = {'result': 'Fail', 'msg': '每次最多提交300个订单号'}
        else:
            try:
                print('心跳....砰..砰..砰.............................')
                for id in orders_id:
                    # 删除订单
                    order = OrdersInfo.objects(id=id).first()
                    if order:
                        order.update(set__updated_at=int(time.time()))
                        if order.status == 4:
                            order.update(set__status=0)
                    else:
                        invalid_orders.append(id)
            except Exception as e:
                print(' 心跳更新订单有效期异常 %s' % e)
                logging.exception(e)
            data = {'result': 'ok', 'msg': '订单有效期已更新', 'invalid_orders': invalid_orders}
        return Response(data, status=status.HTTP_200_OK)


