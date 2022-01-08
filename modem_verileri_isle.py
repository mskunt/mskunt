import psycopg2
import pandas as pd
import base64
import numpy as np
from sqlalchemy import create_engine

import glob
import re
from datetime import datetime 

import mysql.connector 
from sqlalchemy import create_engine
import pymysql


def port_number(stream_text):
    stream_text_info = stream_text.split("/")
    port_number = stream_text_info[-1]
    port_number =re.sub(r'[^0-9]+', '', port_number)
    return port_number

def port_sorunlumu(port_type,cer,ccer,snr,rxpower,txpower):    
    sorunlu_mu = 0
    if port_type == "DS":
        if  cer > 0.5 or ccer > 0.5 or snr < 35 or not (rxpower >= -8 and rxpower <=15) :
            sorunlu_mu = 1
        elif np.isnan(cer) or np.isnan(ccer) or np.isnan(snr) or np.isnan(rxpower):
            sorunlu_mu = np.nan #okunamamış metrikler
    elif port_type == "US":
        if cer > 1 or ccer > 1 or snr < 28 or not (rxpower >= -1 and rxpower <=1) or not(txpower >= 30 and txpower <50):
            sorunlu_mu = 1
        elif np.isnan(cer) or np.isnan(ccer) or np.isnan(snr) or np.isnan(rxpower) or np.isnan(txpower):
            sorunlu_mu = np.nan #okunamamış metrikler
        
    return sorunlu_mu

def modem_mac_temizle(modem_mac):
    modem_mac = re.sub('[^a-fA-F0-9_]', '', modem_mac).upper()
    return modem_mac

def modem_sahiplik_bilgileri(cmts_okuma_zamani):
    pwd = base64.b64decode("ZHc0NTZfIw==").decode("utf-8")
    alchemyEngine   = create_engine("postgresql+psycopg2://dwh:"+pwd+"@10.100.79.21/ktv", pool_recycle=3600);
    dbConnection    = alchemyEngine.connect();

    df_modem_sahiplik = pd.read_sql("""
    select modem_mac,hizmet_id
    from (
	select modem_mac,hizmet_id,row_number() over(partition by modem_mac order by onem_sirasi asc)  as sira 
	from 
	(
		select
		    distinct 
		    modem_mac,hizmet_id,1 as onem_sirasi
		from raporlar.tb_modem_abone_sahiplik 
		where '"""+cmts_okuma_zamani +"""' between ilk_tarih and coalesce(son_tarih,current_timestamp)
		union all
		select distinct 
		    hfc_mac_adresi as modem_mac,id as hizmet_id ,2 as onem_sirasi
		from tb_musteri_hizmet 
		where hfc_mac_adresi is not null
        union all
        select
		    distinct 
		    modem_mac,hizmet_id,3 as onem_sirasi
		from raporlar.tb_modem_abone_sahiplik 
		where '"""+cmts_okuma_zamani +"""' between ilk_tarih and coalesce(son_tarih + INTERVAL '6' hour,current_timestamp)
	)t
)m 
where m.sira = 1
""", dbConnection);
    df_modem_sahiplik["modem_mac"] = df_modem_sahiplik["modem_mac"].apply(modem_mac_temizle)

    dbConnection.close();
    return df_modem_sahiplik

def abone_bina_bilgileri():
    pwd = base64.b64decode("ZHc0NTZfIw==").decode("utf-8")
    alchemyEngine   = create_engine("postgresql+psycopg2://dwh:"+pwd+"@10.100.79.21/ktv", pool_recycle=3600);
    dbConnection    = alchemyEngine.connect();

    df_abone_bina = pd.read_sql("select id as hizmet_id , bina_id from tb_musteri_hizmet", dbConnection);    

    dbConnection.close();
    return df_abone_bina

def sorunlu_port_sayisi(sorunlu_mu):
    return np.sum(sorunlu_mu > 0) #-1 olanları almamak için

def port_bilgilerini_yaz(port_sinyal_bilgileri): 
    db_con = create_engine("mysql+pymysql://KNIME:rapor.turksat007@10.100.76.223/DWH"
                           .format(user="KNIME",
                                   pw="rapor.turksat007",
                                   db="DWH"))
    #df = pd.read_sql("select * from DWH.tb_modem_port_sinyal_bilgileri limit 100", db_con);    

    port_sinyal_bilgileri.to_sql('tb_modem_port_sinyal_bilgileri', con = db_con, if_exists = 'append', chunksize = 5000,index=False)
    
	
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time For Read Data And Merge Start =", current_time)

file_path = glob.glob("D:\\İşler\\modem_sinyal\\test\\*.gz")

print(file_path)

file_pd = pd.DataFrame([(f.split("\\"))[-1].replace(".csv.gz","").replace("DS_","").replace("US_","") for f in file_path],columns=["filename"])
file_pd["path"] = file_path
file_pd["type"] = ["DS" if "DS" in f else "US" for f in file_path]

ds_file = file_pd[file_pd["type"]=="DS"].set_index("filename")
us_file = file_pd[file_pd["type"]=="US"].set_index("filename")

#hem DS hem US dosyası olan saatlere ait dosyaları seçelim.
file_pair = ds_file.join(us_file,lsuffix="_ds",rsuffix="_us")

print(file_pair)
file = file_pair[:1] # test için ilk dosyayı kullanacaz sonra burası loop olacak
print(file)

content_ds = pd.read_csv(file["path_ds"][0],header=None)
content_us = pd.read_csv(file["path_us"][0],header=None)

content_ds.columns=["cmts_name","modem_mac","stream_name","cer","ccer","snr","rxpower","cmts_okuma_zamani"]
content_us.columns=["cmts_name","modem_mac","stream_name","cer","ccer","snr","rxpower","txpower","cmts_okuma_zamani"]
content_ds["port_type"]= "DS"
content_us["port_type"]= "US"

#ds ve us portları tek dataframe de toplayalım.
content = pd.concat([content_ds,content_us],ignore_index=True)

content["port_number"] = content['stream_name'].apply(port_number)
content.drop("stream_name",axis=1,inplace=True)
content["modem_mac"] = content["modem_mac"].apply(modem_mac_temizle)

content["sorunlu_mu"] = list(map(port_sorunlumu,content["port_type"], content["cer"], content["ccer"], content["snr"], content["rxpower"], content["txpower"]))

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time For Read Data And Merge End =", current_time)


cmts_okuma_zamani = content["cmts_okuma_zamani"][0]
#okuma zamanına uygun olan modem sahiplik bilgilerini getir.
df_modem_sahiplik = modem_sahiplik_bilgileri(cmts_okuma_zamani)
#abonelere ait bina bilgileri
df_abone_bina = abone_bina_bilgileri()

#sinyal verileri ile modem sahiplik verilerini modem mac üzerinden joinle. hizmet id alanını alacağız
port_sinyal_bilgileri= content.merge(df_modem_sahiplik, on='modem_mac', how='left')
port_sinyal_bilgileri= port_sinyal_bilgileri.merge(df_abone_bina, on='hizmet_id', how='left')

#port_sinyal_bilgileri[["hizmet_id","bina_id"]] = port_sinyal_bilgileri[["hizmet_id","bina_id"]].fillna("-1").copy()
port_sinyal_bilgileri.dropna(subset=["hizmet_id","bina_id"],inplace=True)

port_sinyal_bilgileri[["hizmet_id","bina_id","port_number"]] = port_sinyal_bilgileri[["hizmet_id","bina_id","port_number"]].astype(int)


port_sinyal_bilgileri = port_sinyal_bilgileri.reindex(columns=['cmts_okuma_zamani','hizmet_id','bina_id','modem_mac','cmts_name', 'cer', 'ccer', 'snr', 'rxpower',
        'port_type', 'txpower', 'port_number',
       'sorunlu_mu'])

grouped = port_sinyal_bilgileri.groupby(["hizmet_id","modem_mac","bina_id","cmts_okuma_zamani","cmts_name"])
modem_sinyal_bilgileri = grouped.agg({'sorunlu_mu':[sorunlu_port_sayisi]
                       })["sorunlu_mu"].reset_index().copy()
					   
modem_sinyal_bilgileri["sorunlu_port_sayisi"] = modem_sinyal_bilgileri["sorunlu_port_sayisi"].astype(int)
modem_sinyal_bilgileri["modem_sorunlu"] = modem_sinyal_bilgileri.apply(lambda x: 1 if x.sorunlu_port_sayisi > 0 else 0,axis=1)


				   
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time Write to DB Port Data Start=", current_time)

#port_bilgilerini_yaz(port_sinyal_bilgileri)

port_sinyal_bilgileri.head()

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time Write to DB Port Data Stop =", current_time)
