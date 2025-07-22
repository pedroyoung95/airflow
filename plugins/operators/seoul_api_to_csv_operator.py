from airflow.hooks.base import BaseHook
import pandas as pd
from airflow.models import BaseOperator

#BaseOperator를 상속받아 custom operator 생성
class SeoulApiToCsvOperator(BaseOperator) :
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt') #template할 수 있는 변수(필드) 선언

    #생성자 overriding
    def __init__(self, dataset_nm, path, file_name, base_dt = None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt

    #실행될 비즈니스 로직을 execute overriding으로 구현
    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}' #공공데이터의 start index, end index만 추가하면 됨

         #1000 row씩 데이터 반복 추출
        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True :
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000 :
                break
            else :
                start_row = end_row + 1
                end_row += 1000
                
        if not os.path.exists(self.path) :
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding = 'utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row) :
        import requests
        import json

        headers = {'Content-type' : 'application/json',
                   'charset' : 'utf-8',
                   'Accept' : '*/*'
                  }
        request_url = f'{base_url}/{start_row}/{end_row}'
        if self.base_dt is not None :
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers) #request url, header 정보를 가지고 웹에 resquest한 결과(response)를 reponse 변수에 저장
        contents = json.loads(response.text) #dictionary형의 웹으로부터 받은 데이터(문자열이여서 text 필드 사용)를 json 양식으로 load

        key_nm = list(contents.keys())[0] #dictionary형의 데이터 중 맨 첫 번째 키 값 저장
        row_data = contents.get(key_nm).get('row') #첫 번째 키 값의 value 내부에서 다시 키 값이 'row'인 value를 저장(실제 웹 데이터 부분)
        row_df = pd.DataFrame(row_data)

        return row_df

