#!/usr/bin/env python3
import sys
import os
import json
import time
import datetime
import asyncio
import aiohttp

import requests
import numpy as np
import pandas as pd

from pathlib import Path
import ipywidgets as widgets
from ipywidgets import interact
from IPython.display import display as disp

class Edinet(object):

    URL = 'https://api.edinet-fsa.go.jp/api/v2'
    TSV_PATH = './tsv'
    DOC_PATH = './doc'

    DOCTYPE = {
        'xbrl': 1,
        'pdf': 2,

    }

    def __init__(self, key_path: str=None) -> None:
        self.subscription_key = {}
        self.headers = None
        self.key_path = key_path

        # 環境変数にEDINET_API_KEYが設定されている場合
        edinet_api_key = os.getenv('EDINET_API_KEY')
        if edinet_api_key:
            self.subscription_key = edinet_api_key

        # 指定されたパスにSubscription-Keyが記載されたJSONファイルがある場合
        elif key_path is not None and os.path.isfile(key_path):
            with open(key_path, 'r') as fr:
                data = json.load(fr)
                self.subscription_key = data.get('Subscription-Key')

            print(f'load subscription key: {'*' * len(self.subscription_key)}')
        else:
            # input text
            input_key = widgets.Password(
                description='Subscription Key:',
                ensure_option=True,
                continuous_update=False
            )
            # ボタン
            submit_button = widgets.Button(
                description='値を渡す',
                button_style='success', 
                tooltip='Subscriptionを入力',
            )

            # 結果表示用ラベル（動作確認のため）
            output_label = widgets.Label(value=f'')

            # 3. イベントハンドラ関数の定義
            def on_button_clicked(b):                
                # Textウィジェットの現在の値を読み取り、グローバル変数に代入
                new_value = input_key.value
                
                # ユーザーへのフィードバック（任意）
                output_label.value = f'Subscription Keyが設定されました!'
                self.subscription_key = new_value

            # 4. イベントとの紐付け
            submit_button.on_click(on_button_clicked)
            disp(widgets.VBox([input_key, submit_button, output_label]))



    def get_doc_json(self, date: str=None, output: bool=False):
        '''
        書類一覧取得APIからJSONデータを取得する
        '''
        date = datetime.date.today().strftime('%Y-%m-%d') if date is None else date
            
        doc_url = f'{Edinet.URL}/documents.json?date={date}&type=2&Subscription-Key={self.subscription_key}'
        ret = requests.get(doc_url, headers=self.headers)

        if ret.status_code == 200 and output:
            with open(f'documents_{date}.json', 'w') as fw:
                json.dump(ret.json(), fw, ensure_ascii=False)
        elif ret.status_code == 404:
            print('status code: 404', file=sys.stderr)
            return False
        else:
            print(f'status_code = {ret.status_code}')


        data = ret.json()
        doc_list = data.get('results')

        # TSVのディレクトリがなければ、作成する。
        if not os.path.isdir(self.TSV_PATH):
            tsv_dir = Path(self.TSV_PATH)
            tsv_dir.mkdir(parents=True, exist_ok=True)


        if doc_list is not None:
            df = pd.DataFrame(doc_list)
            df.to_csv(f'{self.TSV_PATH}/document_list_{date}.tsv.gz', sep='\t', index=None)

            df = df.replace({np.nan: None})
            return df
        else:
            return False


    async def async_get_doc_json(self, session: aiohttp.ClientSession, date: str=None):
        '''
        書類一覧取得APIからJSONデータを取得する
        '''
        date = datetime.date.today().strftime('%Y-%m-%d') if date is None else date
            
        doc_url = f'{Edinet.URL}/documents.json?date={date}&type=2&Subscription-Key={self.subscription_key}'
        print(doc_url)

        # TSVのディレクトリがなければ、作成する。
        save_path_dir = Path(Edinet.TSV_PATH)
        save_path_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_path_dir / f'document_list_{date}.tsv.gz'

        try:
            async with session.get(doc_url) as response:
                print(f'Fetching: {doc_url} (Status: {response.status})')
                content = await response.content.read()
                data = json.loads(content)
                doc_list = data.get('results')
                if doc_list is not None:
                    pd.DataFrame(doc_list).to_csv(save_path, sep='\t', index=None)
        except Exception as e:
            print(f'Error fetching {doc_url}: {e}')
            return None

    async def async_get_docs(self, dates: list[str], sleep_time=5):
        '''
        複数のURLのコンテンツを並列で取得
        '''
        async with aiohttp.ClientSession() as session:
            tasks = [self.async_get_doc_json(session, item) for item in dates]
            time.sleep(sleep_time)
            await asyncio.gather(*tasks)


    def get_document(self, doc_id: str, doc_type: int=1, target_dir: str='.'):
        '''
        書類一覧取得APIからJSONデータを取得する
        '''

        doc_url = f'{Edinet.URL}/documents/{doc_id}?type={doc_type}&Subscription-Key={self.subscription_key}'
        ret = requests.get(doc_url, headers=self.headers)

        # 拡張子を決める
        extension = ''
        if doc_type == 2:
            extension = 'pdf'
        else:
            extension = 'zip'

        if ret.status_code == 200:
            with open(os.path.sep.join([target_dir, f'{doc_id}.{extension}']), 'wb') as fw:
                fw.write(ret.content)
            return True
        else:
            print(f'status_code = {ret.status_code}', file=sys.stderr)
            return False


    async def async_get_document(self, session, doc_id: str, target_dir: str, doc_type: int=1):
        '''
        指定されたURLのコンテンツを取得する非同期関数
        '''

        doc_url = f'{Edinet.URL}/documents/{doc_id}?type={doc_type}&Subscription-Key={self.subscription_key}'
        save_path_dir = Path(target_dir)
        save_path_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_path_dir / f'{doc_id}.zip'
        # print(doc_url)
        try:
            async with session.get(doc_url) as response:
                print(f'Fetching: {doc_url} (Status: {response.status})')
                # content = await response.text()  # ここでレスポンスボディを取得
                content = await response.content.read()
                save_path.write_bytes(content)
        except Exception as e:
            print(f'Error fetching {doc_url}: {e}')
            return None


    async def get_documents(self, doc_ids):
        '''
        複数のURLのコンテンツを並列で取得
        '''

        async with aiohttp.ClientSession() as session:
            tasks = [self.async_get_document(session, doc_id, target_dir) for doc_id, target_dir in doc_ids.items()]
            time.sleep(5)
            await asyncio.gather(*tasks)  # すべてのタスクを非同期で実行



if __name__ == '__main__':
    edinet = Edinet()
    today = datetime.date.today()
    target_date = today + datetime.timedelta(days=-180)
    df = pd.DataFrame()
    while target_date <= today:
        ret = edinet.get_doc_json(date=target_date.strftime('%Y-%m-%d'))
        if ret is not False and len(ret) > 0:
            df = pd.concat([df, ret])
        target_date = target_date + datetime.timedelta(days=+1)

    df.to_csv(f'document_list.tsv', sep='\t', index=None)
