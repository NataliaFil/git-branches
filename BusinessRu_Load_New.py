import libs.stg_open.stg_open as stg
import libs.dwh_open.dwh_open as dwh
from libs.BusinessRu import BusinessruAPI
import json
import datetime as dt
import libs.etl.common_etl as etl
from libs.etl.common_etl import LoadMode
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import time

entity_code = 'bru'
entity_settings = stg.get_settings(entity_code)
base_url = f'https://{entity_settings["acc_name"]}.business.ru/api/rest/'


def api_connect(acc_name, app_id, secret):
    _client = BusinessruAPI(acc_name, app_id, secret)
    _client.repair_token()
    return _client


def bru_stage():
    e = etl.Entity(entity_code)
    cl = api_connect(entity_settings["acc_name"],
                     entity_settings["app_id"],
                     entity_settings["secret"])

    for t in e.entity_tables:
        print(f'Data reading from method {t.table_name} started')
        stg_table_name = t.table_name
        t.clear_buffer_stg()
        _updated_from = None
        if t.load_mode == LoadMode.INCREMENT_LOAD:
            _updated_from = t.last_increment_value
            if _updated_from is None:
                _updated_from = dt.datetime(1900, 1, 1)
        _cols = [x.name for x in t.columns]
        _bln_continue = True
        page_num = 1
        page_batch = []
        batch_size = 1000
        row_cnt = 0
        row_cnt_batch = 0
        get_page = None
        total_rows = 0
        while _bln_continue:
            if t.load_mode == LoadMode.INCREMENT_LOAD:
                get_page = cl.request("get",
                                      t.entity_table_name,
                                      limit=entity_settings["page_size"],
                                      page=page_num,
                                      with_additional_fields=1,
                                      **{"updated[from]": _updated_from.strftime("%d.%m.%Y %H:%M:%S")}
                                      )
            elif t.load_mode == LoadMode.FULL_RELOAD:
                get_page = cl.request("get",
                                      t.entity_table_name,
                                      limit=entity_settings["page_size"],
                                      page=page_num,
                                      with_additional_fields=1
                                      )
            elif t.load_mode == LoadMode.SLICE_RELOAD:
                get_page = cl.request("get",
                                      t.entity_table_name,
                                      limit=entity_settings["page_size"],
                                      page=page_num,
                                      with_additional_fields=1,
                                      **{"updated[from]": t.slice_start_date.strftime("%d.%m.%Y %H:%M:%S")}
                                      )
            page_data_str = json.dumps(get_page)
            page_data = json.loads(page_data_str)
            row_cnt = len(page_data["result"])
            total_rows += row_cnt
            if row_cnt > 0:
                page_batch = page_batch + [tuple([r[c] if c in r else None for c in _cols]) for r in page_data["result"]]
                page_num += 1
                row_cnt_batch += row_cnt
                if row_cnt_batch >= batch_size:
                    stg.write_to_server(page_batch,
                                        stg_table_name,
                                        'tmp',
                                        _cols,
                                        False)
                    row_cnt_batch = 0
                    page_batch.clear()
            else:
                _bln_continue = False
            time.sleep(0.6)

        if row_cnt_batch > 0:
            stg.write_to_server(page_batch,
                                stg_table_name,
                                'tmp',
                                _cols,
                                False)
            page_batch.clear()
        print(f'Data reading from method {t.table_name} finished. {total_rows} rows read')
    e.stage()


def bru_to_dwh():
    e = etl.Entity(entity_code)
    e.to_dwh()


def businessru_goods_movement_report_fill():
    dwh.call_proc('businessru_goods_movement_report_fill')


DagName = os.path.splitext(os.path.basename(__file__))[0]
ScheduleInterval = "0 * * * *"
DagStartDate = dt.datetime(2024, 1, 15)
DagDescription = 'Load and transfer tables from Business.Ru'
DagEmail = Variable.get("all_the_kings_men", deserialize_json=True)["Emails"]
#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': DagEmail,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=5),
    'catchup': False
}

with DAG(
    DagName,
    default_args=default_args,
    max_active_runs=1,
    description=DagDescription,
    schedule_interval=ScheduleInterval,
    start_date=DagStartDate,
    tags=['DWH', 'API', 'BusinessRu'],
) as dag:
    bru_stage = PythonOperator(
        task_id='bru_stage',
        python_callable=bru_stage
    )
    bru_to_dwh = PythonOperator(
        task_id='bru_to_dwh',
        python_callable=bru_to_dwh
    )
    businessru_goods_movement_report_fill = PythonOperator(
        task_id='businessru_goods_movement_report_fill',
        python_callable=businessru_goods_movement_report_fill
    )
    bru_stage >> bru_to_dwh >> businessru_goods_movement_report_fill