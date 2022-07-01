import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import connectdb
import logging
from datetime import date, timedelta, datetime

'''
Google Datawarehouse 의 Service monthly Report 데이터를 생성하는 스크립트입니다.

'''

# 로그 생성
logger = logging.getLogger()

# 로그의 출력 기준 설정
logger.setLevel(logging.INFO)

# log 출력 형식
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# log 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

logger.info('Service Monthly 데이터 생성 시작')

#필요한 일자값 생성
# 인덱스 생성날자(오늘 날짜값 생성) 변수 생성
index_date = (date.today()).strftime("%Y.%m.%d")

# 인덱싱 시간값 변수 생성
timestamp = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

logger.info('DB 데이터 조회 시작')

db = connectdb.Adb()

#knlog
df_center = pd.read_sql(db.get_service_center_query(), db.conn)
df_user = pd.read_sql(db.get_service_user_query(), db.conn)
df_child = pd.read_sql(db.get_service_child_query(), db.conn)
df_employment = pd.read_sql(db.get_service_employment_query(), db.conn)
df_enrollment = pd.read_sql(db.get_service_enrollment_query(), db.conn)
df_devices = pd.read_sql(db.get_service_devices_query(), db.conn)
df_admin = pd.read_sql(db.get_service_admin_query(), db.conn)

#kndata
prod_es_conn = db.get_prod_es_conn()
active_center_list = db.get_kndata_store_list(prod_es_conn, index_date)
df_kndata = pd.read_sql(db.get_service_kndata_query(active_center_list), db.conn)


#신규 가입자 Data Scan
logger.info('월 신규 가입자 Data Scan 시작')

# 1) Center Table : 중복 Center ID 제외
df_mini_center = df_center[['center_id', 'created', 'name', 'kind', 'type__name', 'status', 'address__country__alpha2']] #데이터 수집에 필요한 주요 칼럼만 추출

#중복 center_id 제외
df_mini_center.drop_duplicates(['center_id'], inplace=True)

# 2) Admin Table과 Center Table Merge
df_merge_admin = pd.merge(df_admin, df_mini_center, on='center_id') #center id 기준으로 merge

# 3) Employment Table과 Center Table Merge
df_merge_employment = pd.merge(df_employment, df_mini_center, on='center_id') #center id 기준으로 merge

# Employment Table의 중복 Teacher User ID 제외
df_merge_employment.drop_duplicates(['user_id'], inplace=True)

# # 4) Enrollment Table과 Center Table center id 기준으로 Merge
df_merge_enrollment = pd.merge(df_enrollment, df_mini_center, on='center_id')

# 5) Child Table과 Enrollment Table center_id 기준으로 Merge
df_merge_child = pd.merge(df_child, df_merge_enrollment, on='child_id')
# 중복값 제거
df_merge_child.drop_duplicates(['user_id'], inplace=True)

# 6) 데이터 수집에 필요한 주요 칼럼만 추출
df_mini_admin = df_merge_admin[['user_id', 'center_id', 'name', 'kind', 'type__name', 'status', 'address__country__alpha2']]
df_mini_employment = df_merge_employment[['user_id', 'center_id', 'name', 'kind', 'type__name', 'status', 'address__country__alpha2']]
df_mini_child = df_merge_child[['user_id', 'center_id', 'name', 'kind', 'type__name', 'status', 'address__country__alpha2']]

# 7) Center 정보가 합쳐진 Admin/Employment/Child Table을 하나로 결합
df_sum_list = df_mini_admin.append(df_mini_employment, ignore_index=True)
df_sum_list = df_sum_list.append(df_mini_child, ignore_index=True)

# User ID 기준으로 중복 제외 : User Type이 'admin'인 계정이 Admin Table과 Employment Table에 모두 존재한 기록이 있어, Admin 정보를 남기고 중복 제외
df_sum_list.drop_duplicates(['user_id'], inplace=True)

# 8) 위의 결합된 Table과 User Table user_id 기준으로 Merge
df_merge_user = pd.merge(df_user, df_sum_list, on='user_id', how='left')

# 9) User가 속한 Center 지역을 기준으로 한국/해외 유저 분리
df_kr_user = df_merge_user[df_merge_user['address__country__alpha2'] == 'KR']
df_not_kr_user = df_merge_user[df_merge_user['address__country__alpha2'] != 'KR']

# 10) '가입년월, 유저타입, 원 종류' 3가지를 기준으로 가입자 수 카운트
df_kr_user_group = pd.DataFrame(df_kr_user.groupby(['type', 'kind'])['user_id'].size()).reset_index().rename({'user_id':'user_count'}, axis=1)
df_not_kr_user['kind'] = 'not_kr' # 해외 가입자의 경우, center kind를 분리하지 않고 하나로 통일
df_not_kr_user_group = pd.DataFrame(df_not_kr_user.groupby(['type', 'kind'])['user_id'].size()).reset_index().rename({'user_id':'user_count'}, axis=1)

# 11) 국내 유저/해외 유저 정보 통합
df_final_user = df_kr_user_group.append(df_not_kr_user_group, ignore_index=True)

print(df_final_user)
logger.info('월 신규 가입자 Data Scan 종료')

logger.info('월 신규 가입기관 Data Scan 시작')

#신규 가입 기관 Data Scan

# 1) Center Table : 중복 Center ID 제외
df_unique_center = df_center.drop_duplicates(['center_id'])

# 3) 국가 구분
df_unique_center_kr = df_unique_center[df_unique_center['address__country__alpha2'] == 'KR'] # unique center kr = KR center만 추출
df_unique_center_not_kr = df_unique_center[df_unique_center['address__country__alpha2'] != 'KR'] # unique center not kr = KR 아닌 center 추출

# 4) '기관생성년월, 기관종류' 2가지를 기준으로 생성 기관 수 카운트
df_kr_center_group = pd.DataFrame(df_unique_center_kr.groupby(['kind'])['center_id'].size()).reset_index().rename({'center_id':'center_count'}, axis=1)
df_unique_center_not_kr['kind'] = 'not_kr'
df_not_kr_center_group = pd.DataFrame(df_unique_center_not_kr.groupby(['kind'])['center_id'].size()).reset_index().rename({'center_id':'center_count'}, axis=1)

df_center_group = pd.concat([df_kr_center_group, df_not_kr_center_group], ignore_index = True)

logger.info('월 신규 가입기관 Data Scan 종료')

logger.info('월 실사용기관 Data Scan 시작')
df_kndata = pd.merge(df_kndata, df_center[{'center_id', 'address__country__alpha2'}], how='inner', on='center_id').drop_duplicates('center_id') #center 아이디 기준으로 inner join

df_active_center = df_kndata[df_kndata['address__country__alpha2'] == "KR"]['kind'].value_counts()
df_not_kr_center_count = df_kndata[df_kndata['address__country__alpha2'] != "KR"]['center_id'].count()


df_active_center_set = pd.DataFrame()
df_active_center_set = df_active_center_set.append(df_active_center, ignore_index=True)

df_active_center_set.loc[:, 'not_kr'] = df_not_kr_center_count #2020년 12월 16일 해외 원 데이터 추가

logger.info('월 실사용기관 Data Scan 종료')

logger.info('월 실사용유저 Data Scan 시작')

#User Type 별 월 실사용 유저

# 이전달 1일 얻기
last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
start_day_of_prev_month = (date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)).strftime("%Y-%m-%d")

####################### 이전 달 1일보다 크거나 같은 데이터 조회
df_active_user = df_user[df_user['last_login'] >= start_day_of_prev_month]
df_active_merge = pd.DataFrame(columns = ["user_id", "type"])

#ADMIN #실사용 기관의 원장 중, 지난 달 접속 기록이 존재하는 유저
df_active_center_admin = df_admin[df_admin['center_id'].isin(df_kndata['center_id'])]
df_active_admin = df_active_user[df_active_user['user_id'].isin(df_active_center_admin['user_id'])][['user_id', 'type']]
df_active_merge = df_active_merge.append(df_active_admin, ignore_index=True)

#TEACHER
df_active_employment = df_employment[(~df_employment['date_approved'].isnull())]

df_active_teacher = df_active_user[df_active_user['user_id'].isin(df_active_employment['user_id'])][['user_id', 'type']]
df_active_merge = df_active_merge.append(df_active_teacher, ignore_index=True)

#PARENT
df_active_enrollment = df_enrollment[(~df_enrollment['date_approved'].isnull())]
df_active_child = df_child[df_child['child_id'].isin(df_active_enrollment['child_id'])].drop_duplicates('user_id')
df_active_parent = df_active_user[df_active_user['user_id'].isin(df_active_child['user_id'])][['user_id', 'type']]
df_active_merge = df_active_merge.append(df_active_parent, ignore_index=True)

#drop duplicates
df_active_merge = df_active_merge.drop_duplicates('user_id')

#Total
df_active_merge = df_active_merge.groupby(['type'])['type'].size()
df_active_user_set = pd.DataFrame()
df_active_user_set = df_active_user_set.append(df_active_merge, ignore_index=True)

logger.info('월 실사용유저 Data Scan 종료')

logger.info('os유저 Data Scan 시작')
#위에서 구한 실사용 유저 중, OS별 버전 카운트
#OS버전을 알 수 없는 기기 제외
df_devices_sort = pd.DataFrame(df_devices.groupby(['push_type', 'os']).size().reset_index().rename({'push_type':'os','os':'version', 0:'count'}, axis=1))
df_devices_sort['os'] = df_devices_sort['os'].apply(lambda x : 'ios' if x=='apns' else 'android')
print(df_devices_sort)

logger.info('os유저 Data Scan 종료')

logger.info('비활성 유저 Data Scan 시작')

#해당 월 가입자 추출
df_teacher_filter_user = df_user[(df_user['type'] == 'teacher') & (df_user['date_joined'] >= start_day_of_prev_month)
                                 & (df_user['alpha2'] == 'KR')].drop_duplicates('user_id')
df_parent_filter_user = df_user[(df_user['type'] == 'parent') & (df_user['date_joined'] >= start_day_of_prev_month)
                                & (df_user['alpha2'] == 'KR')].drop_duplicates('user_id')

#해당 월 퇴사자 추출
df_left_teacher = df_employment[(df_employment['user_id'].isin(df_teacher_filter_user['user_id'])) & (df_employment['is_approved'] == False)
                                & (df_employment['date_left'] >= start_day_of_prev_month)].drop_duplicates('user_id')

#해당 월 퇴소자 부모 추출
df_child_filter = df_child[df_child['user_id'].isin(df_parent_filter_user['user_id'])].drop_duplicates('child_id')
df_left_child = df_enrollment[(df_enrollment['child_id'].isin(df_child_filter['child_id'])) & (df_enrollment['is_approved'] == False)
                              & (df_enrollment['date_left'] >= start_day_of_prev_month)].drop_duplicates('child_id')
df_left_parent = df_child_filter[df_child_filter['child_id'].isin(df_left_child['child_id'])].drop_duplicates('user_id')

logger.info('비활성 유저 Data Scan 종료')

es_conn = db.get_es_conn()

'''
Elasticsearch 에 google datasheet data 적재
대상 sheet: 01.4 Service-montly_count의
user_count, center_count, active_center, active_user, android_ver, ios_ver

# sheet.worksheet_by_title('user_count').set_dataframe(final_user_df, 'A1', fit=True)
# sheet.worksheet_by_title('center_count').set_dataframe(center_group, 'A1', fit=True)
sheet.worksheet_by_title('active_center').set_dataframe(active_center_set, 'A1', fit=True)
sheet.worksheet_by_title('active_user').set_dataframe(active_user_set, 'A1', fit=True)
sheet.worksheet_by_title('android_ver').set_dataframe(android_sort, 'A1', fit=True)
sheet.worksheet_by_title('ios_ver').set_dataframe(ios_sort, 'A1', fit=True)
'''

# 인덱스 생성날자(오늘 날짜값 생성) 변수 생성
index_date = (date.today()).strftime("%Y.%m.%d")

logger.info('Elasticsearch user_count index 시작')

bulk(es_conn, db.dataframe_to_json(df_final_user, 'user_count-{date}'.format(date=index_date)), raise_on_error=True)
logger.info('Elasticsearch user_count index 종료')

logger.info('Elasticsearch center_count index 시작')
bulk(es_conn, db.dataframe_to_json(df_center_group, 'center_count-{date}'.format(date=index_date)), raise_on_error=True)
logger.info('Elasticsearch center_count index 종료')

logger.info('Elasticsearch active_center index 시작')
bulk(es_conn, db.dataframe_to_json(df_active_center_set, 'active_center-{date}'.format(date=index_date)), raise_on_error=True)
logger.info('Elasticsearch active_center index 종료')

logger.info('Elasticsearch active_user index 시작')
bulk(es_conn, db.dataframe_to_json(df_active_user_set, 'active_user-{date}'.format(date=index_date)), raise_on_error=True)
logger.info('Elasticsearch active_user index 종료')

logger.info('Elasticsearch device_ver index 시작')
bulk(es_conn, db.dataframe_to_json(df_devices_sort, 'device_ver-{date}'.format(date=index_date)), raise_on_error=True)
logger.info('Elasticsearch device_ver index 종료')

logger.info('Elasticsearch index 종료')

logger.info('Slack 메시지 발송 시작')

logger.info('자동 스크립트 실행 종료')