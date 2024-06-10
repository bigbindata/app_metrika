import requests
import os
from datetime import datetime
from datetime import timedelta
import time

key = os.getenv("API_KEY")
app_id = os.getenv("APP_ID")

start_date = (datetime.now()- timedelta(days=1)).strftime("%Y-%m-%d") # заменить здесь days=1 на days=7 для загрузки за последние 7 дней не включая текущую дату
end_date = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d") # заменить здесь days=1 на days=7 для загрузки за последние 7 дней не включая текущую дату

class ApiAppMetrika():

  def __init__(self,key=os.getenv("API_KEY"),app_id = os.getenv("APP_ID")):
    self.key = key
    self.app_id = app_id
    self.base_url = "https://api.appmetrica.yandex.ru/"
    self.version_api = "v1"
    self.format = 'json'
    self.headers = {'Authorization': f"OAuth {key}"}
    self.config_data = {'clicks':'application_id,click_datetime,click_id,click_ipv6,click_timestamp,click_url_parameters,click_user_agent,publisher_id,publisher_name,tracker_name,tracking_id,touch_type,is_bot,city,country_iso_code,device_manufacturer,device_model,device_type,google_aid,oaid,ios_ifa,ios_ifv,os_name,os_version,windows_aid',

           'installations':'application_id,installation_id,attributed_touch_type,click_datetime,click_id,click_ipv6,click_timestamp,click_url_parameters,click_user_agent,profile_id,publisher_id,publisher_name,tracker_name,tracking_id,install_datetime,install_ipv6,install_receive_datetime,install_receive_timestamp,install_timestamp,is_reattribution,is_reinstallation,match_type,appmetrica_device_id,city,connection_type,country_iso_code,device_locale,device_manufacturer,device_model,device_type,google_aid,oaid,ios_ifa,ios_ifv,mcc,mnc,operator_name,os_name,os_version,windows_aid,app_package_name,app_version_name',

           'postbacks':'application_id,attributed_touch_type,click_datetime,click_id,click_ipv6,click_timestamp,click_url_parameters,click_user_agent,publisher_id,publisher_name,tracker_name,tracking_id,install_datetime,install_ipv6,install_timestamp,match_type,appmetrica_device_id,device_locale,device_manufacturer,device_model,device_type,google_aid,oaid,ios_ifa,os_name,os_version,windows_aid,app_package_name,app_version_name,conversion_datetime,conversion_timestamp,event_name,attempt_datetime,attempt_timestamp,cost_model,notifying_status,postback_url,postback_url_parameters,response_body,response_code',

           'events':'event_datetime,event_json,event_name,event_receive_datetime,event_receive_timestamp,event_timestamp,session_id,installation_id,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,original_device_model,os_name,os_version,profile_id,windows_aid,app_build_number,app_package_name,app_version_name,application_id',

           'profiles':'profile_id,appmetrica_gender,appmetrica_birth_date,appmetrica_notifications_enabled,appmetrica_name,appmetrica_crashes,appmetrica_errors,appmetrica_first_session_date,appmetrica_last_start_date,appmetrica_push_opens,appmetrica_push_send_count,appmetrica_sdk_version,appmetrica_sessions,android_id,appmetrica_device_id,city,connection_type,country_iso_code,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,os_name,os_version,windows_aid,app_build_number,app_framework,app_package_name,app_version_name',

           'revenue_events':'revenue_quantity,revenue_price,revenue_currency,revenue_product_id,revenue_order_id,revenue_order_id_source,is_revenue_verified,event_datetime,event_name,event_receive_datetime,event_receive_timestamp,event_timestamp,session_id,installation_id,android_id,appmetrica_device_id,appmetrica_sdk_version,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,event_datetime,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,original_device_model,os_version,profile_id,windows_aid,app_build_number,app_package_name,app_version_name',

           'deeplinks':'deeplink_url_host,deeplink_url_parameters,deeplink_url_path,deeplink_url_scheme,event_datetime,event_receive_datetime,event_receive_timestamp,event_timestamp,is_reengagement,profile_id,publisher_id,publisher_name,session_id,tracker_name,tracking_id,android_id,appmetrica_device_id,appmetrica_sdk_version,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,original_device_model,os_version,windows_aid,app_build_number,app_package_name,app_version_name',

           'crashes':'crash,crash_datetime,crash_group_id,crash_id,crash_name,crash_receive_datetime,crash_receive_timestamp,crash_timestamp,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,os_name,os_version,profile_id,windows_aid,app_package_name,app_version_name,application_id',

           'errors':'error,error_datetime,error_id,error_name,error_receive_datetime,error_receive_timestamp,error_timestamp,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,os_name,os_version,profile_id,windows_aid,app_package_name,app_version_name,application_id',

           'push_tokens':'token,token_datetime,token_receive_datetime,token_receive_timestamp,token_timestamp,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,os_name,os_version,profile_id,windows_aid,app_package_name,app_version_name,application_id',

           'sessions_starts':'session_id,session_start_datetime,session_start_receive_datetime,session_start_receive_timestamp,session_start_timestamp,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,original_device_model,os_name,os_version,profile_id,windows_aid,app_build_number,app_package_name,app_version_name,application_id'
           }

  def get_data_from_app_metrika(self, date_since: str, date_until: str, name_report:str ,format = None) -> str|list:
    if not format:
      format = self.format
    response = requests.get(
      url=self.base_url + 'logs/' + self.version_api + "/export/" + name_report + "." + format,
      headers=self.headers,
      params={
          'application_id' : self.app_id,
          'date_since': date_since,
          'date_until': date_until,
          'fields': self.config_data[name_report],
      }
                        )
    if response.status_code == 202:
        return response.text
    elif response.status_code == 200:
        return response.json()['data']

api_app_metrika = ApiAppMetrika()
api_app_metrika

done_dict = {}
for name_report in api_app_metrika.config_data.keys():
    done_dict[name_report] = api_app_metrika.get_data_from_app_metrika(start_date,end_date,name_report = name_report)

time.sleep(60)

for name_report in api_app_metrika.config_data.keys():
    if type(done_dict[name_report]) == str:
        done_dict[name_report] = api_app_metrika.get_data_from_app_metrika(start_date,end_date,name_report = name_report)


time.sleep(30)

for name_report in api_app_metrika.config_data.keys():
    if type(done_dict[name_report]) == str:
        done_dict[name_report] = api_app_metrika.get_data_from_app_metrika(start_date,end_date,name_report = name_report)

time.sleep(60)

for name_report in api_app_metrika.config_data.keys():
    if type(done_dict[name_report]) == str:
        done_dict[name_report] = api_app_metrika.get_data_from_app_metrika(start_date,end_date,name_report = name_report)

for name_report in api_app_metrika.config_data.keys():
    if type(done_dict[name_report]) == str:
        print ("Не скачан отчет ", name_report)
