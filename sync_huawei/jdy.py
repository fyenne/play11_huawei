import numpy as np
import pandas as pd
import requests
import time
import json
import psycopg2
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


class APIUtils:

    WEBSITE = "https://www.jiandaoyun.com"
    RETRY_IF_LIMITED = True

    # 构造函数
    def __init__(self, appId, entryId, api_key):
        self.url_get_widgets = APIUtils.WEBSITE + '/api/v1/app/' + appId + '/entry/' + entryId + '/widgets'
        self.url_get_data = APIUtils.WEBSITE + '/api/v1/app/' + appId + '/entry/' + entryId + '/data'
        self.url_retrieve_data = APIUtils.WEBSITE + '/api/v1/app/' + appId + '/entry/' + entryId + '/data_retrieve'
        self.url_update_data = APIUtils.WEBSITE + '/api/v1/app/' + appId + '/entry/' + entryId + '/data_update'
        self.url_create_data = APIUtils.WEBSITE + '/api/v1/app/' + appId + '/entry/' + entryId + '/data_create'
        self.url_delete_data = APIUtils.WEBSITE + '/api/v1/app/' + appId + '/entry/' + entryId + '/data_delete'
        self.api_key = api_key

    # 带有认证信息的请求头
    def get_req_header(self):
        return {
            'Authorization': 'Bearer ' + self.api_key,
            'Content-Type': 'application/json;charset=utf-8'
        }

    # 发送http请求
    def send_request(self, method, request_url, data):
        proxies = {"http": "http://23.210.15.233:8080", "https": "https://23.210.15.233:8080"}
        headers = self.get_req_header()
        if method == 'GET':
            res = requests.get(request_url, params=data, headers=headers, verify=False)
        if method == 'POST':
            res = requests.post(request_url, data=json.dumps(data), headers=headers, verify=False)
        result = res.json()
        if res.status_code >= 400:
            if result['code'] == 8303 and APIUtils.RETRY_IF_LIMITED:
                # 5s后重试
                time.sleep(5)
                return self.send_request(method, request_url, data)
            else:
                raise Exception('请求错误！', result)
        else:
            return result

    # 获取表单字段
    def get_form_widgets(self):
        result = self.send_request('POST', self.url_get_widgets, {})
        return result['widgets']

    # 根据条件获取表单中的数据
    def get_form_data(self, dataId, limit, fields, data_filter):
        result = self.send_request('POST', self.url_get_data, {
            'data_id': dataId,
            'limit': limit,
            'fields': fields,
            'filter': data_filter
        })
        return result['data']

    # 获取表单中满足条件的所有数据
    def get_all_data(self, fields, data_filter):
        form_data = []

        # 递归取下一页数据
        def get_next_page(dataId):
            data = self.get_form_data(dataId, 100, fields, data_filter)
            if data:
                for v in data:
                    form_data.append(v)
                dataId = data[len(data) - 1]['_id']
                get_next_page(dataId)
        get_next_page('')
        return form_data

    # 检索一条数据
    def retrieve_data(self, dataId):
        result = self.send_request('POST', self.url_retrieve_data, {
            'data_id': dataId
        })
        return result['data']

    # 创建一条数据
    def create_data(self, data):
        result = self.send_request('POST', self.url_create_data, {
            'data': data
        })
        return result['data']

    # 更新数据
    def update_data(self, dataId, data):
        result = self.send_request('POST', self.url_update_data, {
            'data_id': dataId,
            'data': data
        })
        return result['data']

    # 删除数据
    def delete_data(self, dataId):
        result = self.send_request('POST', self.url_delete_data, {
            'data_id': dataId
        })
        return result

    def createJdyIDbyPK(self,project,customer_name,cost_center,invoice_money_done):
        data = {
            'project': {
                'value': project
            },
            'customer_name': {
                'value': customer_name
            },
            'cost_center': {
                'value': cost_center
            },
            'invoice_money_done': {
                'value': invoice_money_done
            }
        }
        create_data = self.create_data(data)
        return create_data['_id']


    def createJdyIDbyReceiveRow(self,v):
        data = {
            'project': {
                'value':  v['project_text']
            },
            'customer_name': {
                'value':  v['customer_name']
            },
            'cost_center': {
                'value':  v['profit_center']
            },
            'invoice_money_done': {
                'value':  v['sum_voucher_amount_uncleared']
            },
            'company_code': {
                'value': v['company_code']
            },
            'customer_code': {
                'value': v['customer_number']
            },
            'evidence_code': {
                'value': v['voucher_no']
            },
            'invoice_number': {
                'value': v['fphm']
            },
            'baseline_date': {
                'value': v['baseline_date']
            },
            'overdue_date': {
                'value': v['overdue_date']
            },
            'bg': {
                'value': v['bg']
            }
        }
        create_data = self.create_data(data)
        return create_data['_id']

    def createJdyIDbyEstimateRow(self,v):
        data = {
            'project': {
                'value':  v['project_text']
            },
            'customer_name': {
                'value':  v['customer_name']
            },
            'cost_center': {
                'value':  v['profit_center']
            },
            'invoice_money_done': {
                'value':  v['voucher_amount']
            },
            'company_code': {
                'value': v['company_code']
            },
            'customer_number': {
                'value': v['customer_number']
            },
            'voucher_no': {
                'value': v['voucher_no']
            },
            'overdue_days': {
                'value': v['overdue_days']
            },
            'update_currency_general_ledger': {
                'value': v['update_currency_general_ledger']
            },
            'amount_of_local_currency': {
                'value': v['amount_of_local_currency']
            },
            'assignment_date': {
                'value': v['assignment_date']
            },
            'cc': {
                'value': v['overdue_type']
            },
            'bg': {
                'value': v['bg']
            },
            'clear_type': {
                'value': v['clear_type']
            }
        }
        create_data = self.create_data(data)
        return create_data['_id']


    def insertValuesbyEstimateRow(self,v):
        data = {
            'project': {
                'value':  v['project_text']
            },
            'customer_name': {
                'value':  v['customer_name']
            },
            'cost_center': {
                'value':  v['profit_center']
            },
            'invoice_money_done': {
                'value':  v['voucher_amount']
            },
            'company_code': {
                'value': v['company_code']
            },
            'customer_number': {
                'value': v['customer_number']
            },
            'voucher_no': {
                'value': v['voucher_no']
            },
            'overdue_days': {
                'value': v['overdue_days']
            },
            'update_currency_general_ledger': {
                'value': v['update_currency_general_ledger']
            },
            'amount_of_local_currency': {
                'value': v['amount_of_local_currency']
            },
            'assignment_date': {
                'value': v['assignment_date']
            },
            'cc': {
                'value': v['overdue_type']
            },
            'bg': {
                'value': v['bg']
            },
            'clear_type': {
                'value': v['clear_type']
            }
        }

        try: self.update_data(v['jdy_data_id'],data)
        except:print('fail to insert values with id %s'%(v['jdy_data_id']))

    def insertValuesbyReceiveRow(self,v):
        data = {
            'project': {
                'value':  v['project_text']
            },
            'customer_name': {
                'value':  v['customer_name']
            },
            'cost_center': {
                'value':  v['profit_center']
            },
            'invoice_money_done': {
                'value':  v['sum_voucher_amount_uncleared']
            },
            'company_code': {
                'value': v['company_code']
            },
            'customer_code': {
                'value': v['customer_number']
            },
            'evidence_code': {
                'value': v['voucher_no']
            },
            'invoice_number': {
                'value': v['fphm']
            },
            'baseline_date': {
                'value': v['baseline_date']
            },
            'overdue_date': {
                'value': v['overdue_date']
            },
            'bg': {
                'value': v['bg']
            }
        }

        try: self.update_data(v['jdy_data_id'],data)
        except:print('fail to insert values with id %s'%(v['jdy_data_id']))
