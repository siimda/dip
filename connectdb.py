import pymysql
from serverinfo import BASE_SERVER
from elasticsearch import Elasticsearch

'''
Mysql, Elasticsearch 접속 정보 및 초기데이터 추출 쿼리를 취합한 소스입니다.
'''

class Adb(object):
   def __init__(self, server='main'):
       self.server = server
       self.conn, self.cs = None, None
       if not self.cs:
           self.setup()

   def setup(self):
       if self.server in BASE_SERVER:
           self._setup_mysql()

   def _setup_mysql(self):
       self.conn = pymysql.connect(host=BASE_SERVER[self.server]['host'],
                                   user=BASE_SERVER[self.server]['user'],
                                   passwd=BASE_SERVER[self.server]['pw'],
                                   db=BASE_SERVER[self.server]['db'],
                                   port=BASE_SERVER[self.server]['port'],
                                   charset="utf8",
                                   cursorclass=pymysql.cursors.DictCursor)
       self.conn.autocommit(False)
       self.cs = self.conn.cursor()
       self.conn, self.cs = self.conn, self.cs

   def get_es_conn(self):
       host = 'http://localhost:9200'
       es_conn = Elasticsearch(host)
       return es_conn

   # pandas dataframe을 elasticsearch에 적재하기 위해 json 형태로 바꿔주는 메서드
   def dataframe_to_json(self, df, index_name):
       for index, document in df.iterrows():
           yield {
               "_index": index_name,
               "_id": index,
               "_source": {key: document[key] for key in list(df.columns)}
           }

   def get_pod_item_query(self):
       '''
       컬럼명 수정, drop 하는 컬럼 안가져오도록 수정 및 부가세 처리
       '상품 준비중','배송 준비중','배송중','배송 완료','부분 환불' 상태인 데이터만 가져오도록 처리
       '''

       sql = """select
                `Item ID` as item_id
                ,`Order ID` as order_id
                #,`Order Date` as order_date
                ,`User ID` as user_id
                ,`Product SKU` as product_sku
                #,`Product ID ( PK )` as product_id
                ,`Product Name` as product_name
                ,`Vendor ID` as vendor_id
                ,`Price Sale`/1.1 as price_sale
                ,`Product Option` as product_option
                ,`Sum Price Sale`/1.1 as sum_price_sale
                ,`Product Quantity` as product_quantity
                ,`Product Sale Price`/1.1 as product_sale_price
                ,`Product Discount Price (coupon)`/1.1 as product_discount_price
                ,`Product Refund Quantity` as product_refund_quantity
                ,`Product Refund Payment Price`/1.1 as  product_refund_payment_price
                ,`Product Settle Price`/1.1 as revenue
                ,`Product Status` as product_status
                ,`배송 상태`
                ,`Invoice Date`
                ,`Shipping Name`
                ,`Invoice Number`
                ,`Pages` as pages
                ,`User ID.1` as username
                ,`xxSpace ID` as ws_id
             from testDB.item
             where `Product Status` in ("상품 준비중", 
                                                "배송 준비중", 
                                                "배송중", 
                                                "배송 완료", 
                                                "부분 환불")   
            """

       return sql

   def get_pod_order_query(self):
       '''
       컬럼명 수정, drop 하는 컬럼 안가져오도록 수정 및 부가세 처리 /1.1 처리
       '''
       sql = '''
         select 
             `Order ID` as order_id
             ,`Order Number` as order_number
             ,`Order Date` as order_date
             ,`Order Status` as order_status
             ,`User Username` as user_username
             ,`User ID` as user_id
             ,`Order Quantity` as order_quantity
             ,`Order Payment Price`/1.1 as order_payment_price
             ,`Order Discount Price`/1.1 as order_discount_price
             ,`Order Shipping Cost`/1.1 as order_shipping_cost
             ,`Order Payment Amount`/1.1 as order_payment_amount
             ,`Order Refund Payment Price`/1.1 as order_refund_payment_price
             ,`Order Settle Amount`/1.1 as order_settle_amount
             ,`Payment Method` as payment_method
             ,`Payment Date` as payment_date
         from testDB.order_table
         '''

       return sql

   def get_pod_refund_query(self):
       '''
       컬럼명 수정, drop 하는 컬럼 안가져오도록 수정 및 부가세 처리 /1.1 처리
       '''
       sql = '''
         select 
              `Refund ID` as refund_id
              ,`Refund Date` as refund_date
              ,`Item ID` as item_id
              ,`Refund Quantity` as refund_quantity
              ,`Refund Shipping Cost`/1.1 as refund_shipping_cost
              ,`Refund Payment Price`/1.1 as refund_payment_price
              ,`Status`
         from testDB.refund
         '''

       return sql

   def get_pod_ws_info_query(self):
       sql = '''
         select 
             `Workspace ID` as workspace_id
             ,`User ID` as username
             ,`User PK` as user_id
             ,`선택한 유아` as child_info
             ,`책장 기간`
             ,`생성일` as created_time
             ,`수정일` as recently_modified_time
             ,`Cart 등록 여부` as is_in_cart
             ,`알림장 개수(가정에서)`
             ,`알림장 개수(원에서)`
             ,`적은 본문 알림장 개수`
             ,`사진 없는 알림장 개수`
             ,`알림장 댓글 개수`
             ,`앨범 개수`
             ,`비율차 사진 개수`
             ,`Device - Model` as device_model
             ,`Device - Version` as device_version
             ,`Device - OS` as device_os
             ,`Device - Device ID` as device_id
         from testDB.workspace_info 
         '''

       return sql

   def get_pod_ws_step_query(self):
       sql = '''
         select 
             `Workspace ID` as ws_id
             ,Status as ws_status
             ,`[R-071-1] 알림장 제외` as filter1_1
             ,`[R-071-1] 앨범 제외` as filter1_2
             ,`[R-071-2] 가정에서 보낸 알림장 제외` as filter2
             ,`[R-071-3] 첨부파일 없는 알림장을 제외` as filter3
             ,`[R-071-4] 적은 본문 알림장 제외` as filter4
             ,`[R-071-5] 댓글 제외` as filter5
             ,`[R-071-6]사진 비율 1:2.5 비율 사진 제외?` as filter6 
             ,`[R-07x] Task 진행율` as task_progress
         from 
         testDB.workspace_step        
         '''

       return sql

   def get_pod_moments_query(self):
       sql = '''
         '''

       return sql

   def get_service_center_query(self):
       sql = '''
         SELECT
             `centers_center`.`id` as center_id,
             `centers_center`.`created`,
             `centers_center`.`name`, 
             `centers_center`.`kind`,
             `centers_type`.`name` as type__name,
             `centers_center`.`phone`,
             `centers_portal_center`.`code` as portal_center__code,
             `address_country`.`alpha2` as address__country__alpha2,
             `centers_center_address`.`state` as address__state,
             `centers_center_address`.`city` as address__city,
             `centers_admin`.`user_id` as admin__user_id,
             `centers_center`.`status`
         FROM
             kidsdb.`centers_center`
         LEFT OUTER JOIN
             kidsdb.`centers_type` ON (`centers_center`.`type_id` = `centers_type`.`id`)
         LEFT OUTER JOIN
             kidsdb.`centers_portal_center` ON (`centers_center`.`portal_center_id` = `centers_portal_center`.`id`)
         LEFT OUTER JOIN
             kidsdb.`centers_center_address` ON (`centers_center`.`id` = `centers_center_address`.`center_id`)
         LEFT OUTER JOIN
             kidsdb.`address_country` ON (`centers_center_address`.`country_id` = `address_country`.`id`)
         LEFT OUTER JOIN
             kidsdb.`centers_admin` ON (`centers_center`.`id` = `centers_admin`.`center_id`)
             '''

       return sql

   def get_service_user_query(self):
       sql = '''
         SELECT
             `users_user`.`id` as user_id,
             `users_user`.`username`,
             `users_user`.`type`,
             `users_user`.`phone`,
             `users_user`.`date_joined`,
             `users_user`.`last_login`,
             `users_user`.`subscription_updated_at`,
             `address_country`.`alpha2`,
             `users_user`.`subscription`,
             `users_user`.`third_party_consent`
         FROM
             kidsdb.`users_user`
         INNER JOIN
             kidsdb.`address_country` ON (`users_user`.`country_id` = `address_country`.`id`)
         '''

       return sql

   def get_service_child_query(self):
       sql = '''
        SELECT
             `families_child`.`id` as child_id,
             `families_child`.`date_birth`,
             `families_child`.`gender`,
             `families_parent`.`user_id` as user_id
         FROM
             kidsdb.`families_child`
         INNER JOIN kidsdb.`families_parent` ON (`families_child`.`parent_id` = `families_parent`.`id`) 
         '''

       return sql

   def get_service_employment_query(self):
       sql = '''
         SELECT
             `employments_employment`.`id`,
             `employments_employment`.`teacher_id`,
             `employments_employment`.`center_id`,
             `employments_employment`.`class_in_charge_id`,
             `teachers_teacher`.`user_id` as user_id,
             `employments_employment`.`date_approved`,
             `employments_employment`.`is_approved`,
             `employments_employment`.`date_left`
         FROM
             kidsdb.`employments_employment`
         INNER JOIN
             kidsdb.`teachers_teacher` ON (`employments_employment`.`teacher_id` = `teachers_teacher`.`id`)
         ORDER BY
             kidsdb.`employments_employment`.`id` DESC    
         '''

       return sql

   def get_service_enrollment_query(self):
       sql = '''
         SELECT
             `enrollments_enrollment`.`id`,
             `enrollments_enrollment`.`child_id`,
             `enrollments_enrollment`.`center_id`,
             `enrollments_enrollment`.`belong_to_class_id`,
             `enrollments_enrollment`.`date_approved`,
             `enrollments_enrollment`.`is_approved`,
             `enrollments_enrollment`.`date_left`
         FROM
             kidsdb.`enrollments_enrollment`
         '''

       return sql

   def get_service_devices_query(self):
       sql = '''
         SELECT
             `push_device`.`id`,
             `push_device`.`user_id`,
             `push_device`.`app_version`,
             `push_device`.`os`,
             `push_device`.`push_type`,
             `push_device`.`last_login`,
             `push_device`.`last_logout`
         FROM
             kidsdb.`push_device`        
         '''

       return sql

   def get_service_admin_query(self):
       sql = '''
         SELECT
             `centers_admin`.`id`,
             `centers_admin`.`user_id`,
             `centers_admin`.`center_id`
         FROM
             kidsdb.`centers_admin` 
         '''

       return sql

   def get_service_kndata_query(self, center_list):
       sql = '''
              SELECT
                  `centers_center`.`id` as center_id,
                  `centers_center`.`created`,
                  `centers_center`.`modified`,
                  `centers_center`.`name`,
                  `centers_center`.`kind`,
                  `centers_center`.`type_id`,
                  `centers_center`.`num_registration`,
                  `centers_center`.`unique_key`,
                  `centers_center`.`portal_center_id`,
                  `centers_center`.`phone`,
                  `centers_center`.`language`,
                  `centers_center`.`homepage_domain`,
                  `centers_center`.`main_admin_user_id`,
                  `centers_center`.`hash`,
                  `centers_center`.`status`,
                  `centers_center`.`app_type`
              FROM
                  kidsdb.`centers_center`
              WHERE
                  kidsdb.`centers_center`.`id` IN ({center_list})
              '''.format(center_list=center_list)
       return sql

   def get_prod_es_conn(self):
       host = 'http://10.52.2.105:9200'
       es_conn = Elasticsearch(host)

       return es_conn

   def get_kndata_store_list(self, es_conn, index_date):

       # logstash- 인덱스에서 활성 점 id값 조회
       res = es_conn.search(index="logstash-{date}".format(date=index_date), body={
           "size": 0,
           "query": {
               "bool": {
                   "must": [{
                       "terms": {"menu": ["report", "album", "notice"]}
                   }, {
                       "term": {"action": "create"}
                   }]
               }
           },
           "aggs": {
               "unique_ids": {
                   "terms": {"field": "center", "size": 50000}
               }
           },
           "_source": False
       })

       # loop 돌면서 center_id값 뽑아와서 쿼리스트링 'center_id','center_id2' 식으로 생성
       center_list = []
       for center in res['aggregations']['unique_ids']['buckets']:
           center_list.append(str(center['key']))

       center_list_string = ','.join(center_list)

       return center_list_string
