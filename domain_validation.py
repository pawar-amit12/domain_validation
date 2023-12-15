import pandas as pd
import os
import asyncio
import requests
import multiprocessing as mp
from multiprocessing import Pool
import csv
import logging
import datetime
import math

SERVER='1'
INPUT_FILE_PATH_NAME = '/home/ec2-user/scripts/input/domain_'+SERVER+'.csv'
INPUT_FILE_PATH_NAME_NEW = '/home/ec2-user/scripts/input/domain_'+SERVER+'_v2.csv'
FILE_PATH_NAME_TMP= '/home/ec2-user/scripts/output/domain_output_'+SERVER+'.csv'
CHUNCK_SIZE=30
N_PROCESS=30
def main():
    df = pd.read_csv(INPUT_FILE_PATH_NAME_NEW, chunksize=CHUNCK_SIZE)
    process_(df)

def clean_url(url):
    cleaned_url=''
    for u in url.split("."):
        if u !='':
            cleaned_url +=u+'.'

    cleaned_url = cleaned_url.rstrip('.')
    return cleaned_url

def validate_url(url,employer, domain_source):
    # print(url,employer, domain_source)
    try:
        session = requests.Session()
        actual_url = url
        if 'http' not in url:
            url = 'https://' + str(clean_url(url))
        try:
            print('URL:', url)
            request = session.get(url, timeout=30)
            if request.status_code in (200,403,401, 406, 429, 502, 412, 555, 520, 999, 418, 463, 504, 452, 447, 408, 444, 302, 501):
                print(f'{url} with status code {request.status_code} is Valid')
                return {'domain':actual_url,'employer':employer,'domain_source':domain_source , 'status':'valid','status_code':request.status_code,'exception':''}
            else:
                print(f'{url} with status code {request.status_code} is Invalid')
                return {'domain':actual_url,'employer':employer,'domain_source':domain_source, 'status':'invalid','status_code':request.status_code,'exception':''}
        except requests.exceptions.Timeout:
            return {'domain':actual_url,'employer':employer,'domain_source':domain_source,'status':'timeout','status_code':'408', 'exception':''}
        except requests.exceptions.RequestException as e:
            print({'domain':actual_url,'employer':employer,'domain_source':domain_source,'status':'bad request','status_code':'400', 'exception':e} )
            return {'domain':actual_url,'employer':employer,'domain_source':domain_source,'status':'bad request','status_code':'400', 'exception':e}
    except:
        #print(f'{url} with status code {request.status_code} is Invalid')
        return {'domain':actual_url,'employer':employer,'domain_source':domain_source, 'status':'invalid','status_code':'400','exception':'Invalid URL'}

def process_request(index, items, employer, domain_source):
    # print(index,'====', items, employer, domain_source)
    response = validate_url(items, employer, domain_source)
    process_response(response)

def process_response(response):
    # print('response:', response)  
    if response:
        # df_final_tmp = pd.DataFrame.from_dict(response)
        df_final_tmp = pd.DataFrame([response])
        # print(df_final_tmp.head())
        with open(FILE_PATH_NAME_TMP, 'a') as f:
            if not os.path.isfile(FILE_PATH_NAME_TMP):
                df_final_tmp.to_csv(f, index=False)
            else: # else it exists so append without writing the header
                df_final_tmp.to_csv(f, mode='a', header=f.tell()==0, index=False)

def process_(df):
    for enum, chunck_df in enumerate(df):
        items = [(index, row['bq_organization_website_domain'], row['employer'], row['domain_source']) for index, row in chunck_df.iterrows()]
        with Pool() as pool:
            # print('\n------------------------------')
            # print(items)
            result = pool.starmap_async(process_request, items)
            for result in result.get():
                print(f'Got result: {result}', flush=True)


if __name__ == "__main__":
    start_db_write = datetime.datetime.now()
    print('Script starts here ....', start_db_write)
    df_i = pd.read_csv(INPUT_FILE_PATH_NAME, index_col=False)
    df_o = pd.read_csv(FILE_PATH_NAME_TMP, index_col=False)
    print(df_i.shape, df_o.shape)
    df_ = df_i[~df_i['bq_organization_website_domain'].isin(df_o['domain'].unique())]
    print(df_.shape)
    df_.to_csv(INPUT_FILE_PATH_NAME_NEW, index=False)
    main()
    end_db_write = datetime.datetime.now()
    print(f'\nTime taken to complete : {(end_db_write-start_db_write).seconds} seconds')