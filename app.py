import streamlit as st
import pandas as pd
import requests
import re
import io
import time

def clean_url(url):
    if pd.isna(url):
        return ""
    url = str(url).strip().lower()
    url = re.sub(r'^[a-z0-9]+:\/\/', '', url)
    url = re.sub(r'^www\d*\.', '', url)
    url = url.split('/')[0]
    url = url.split('?')[0]
    url = url.split('#')[0]
    url = url.split(':')[0]
    return url.strip()

def get_dataloom_traffic(domain, api_key):
    url = "https://similarweb-insights.p.rapidapi.com/traffic"
    querystring = {"domain": domain}
    headers = {
        "x-rapidapi-host": "similarweb-insights.p.rapidapi.com",
        "x-rapidapi-key": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        requests_left = response.headers.get('x-ratelimit-requests-remaining', 'Неизвестно')
        
        if response.status_code == 200:
            data = response.json()
            
            def find_visits(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k.lower() in ['visits', 'total_visits', 'traffic', 'monthly_visits']:
                            return v
                    for k, v in obj.items():
                        res = find_visits(v)
                        if res is not None:
                            return res
                elif isinstance(obj, list):
                    for item in obj:
                        res = find_visits(item)
                        if res is not None:
                            return res
                return None

            traffic = find_visits(data)
            
            if traffic is not None:
                return traffic, requests_left
            else:
                return "0", requests_left
                
        elif response.status_code == 429:
            return "Лимит исчерпан", 0
        elif response.status_code in [401, 403]:
            return "Ошибка ключа", requests_left
        else:
            return f"Ошибка {response.status_code}", requests_left
            
    except Exception as e:
        return "Ошибка сети", "Неизвестно"

# --- ИНТЕРФЕЙС ---
st.set_page_config(page_title="DataLoom Traffic Checker", layout="wide")
st.title("Сбор трафика через DataLoom (RapidAPI)")

st.sidebar.header("Настройки доступа")
api_key = st.sidebar.text_input("Введи твой x-rapidapi-key", type="password")

uploaded_file = st.file_uploader("Загрузите ваш CSV файл со списком сайтов", type=["csv"])

if uploaded_file:
    if not api_key:
        st.warning("Пожалуйста, введите ключ API в боковом меню слева.")
        st.stop()
        
    with st.spinner('Чтение файла...'):
        # Читаем файл без заголовка, чтобы не потерять первый сайт
        user_df = pd.read_csv(uploaded_file, header=None)
        
        if user_df.empty:
            st.error("Файл пуст.")
            st.stop()
            
        # Проверка: если первая строка случайно оказалась текстовым заголовком, удаляем ее
        first_val = str(user_df.iloc[0, 0]).lower().strip()
        if first_val in ['url', 'domain', 'website', 'сайт', 'домен', 'ссылка']:
            user_df = user_df.iloc[1:].reset_index(drop=True)
            
        user_df['_original_order'] = range(len(user_df))
        site_column = user_df.columns[0]
        
        # Переименовываем первую колонку для красивого вывода
        user_df.rename(columns={site_column: 'Domain'}, inplace=True)
        
        user_df['Clean_Domain'] = user_df['Domain'].apply(clean_url)
        user_df['Visits'] = '-'
        
    if st.button("Начать сбор трафика", type="primary"):
        total_domains = len(user_df)
        st.info(f"Запускаем проверку для всех {total_domains} сайтов...")
        
        domains_to_check = user_df['Clean_Domain'].tolist()
        progress_bar = st.progress(0, text="Опрашиваем API...")
        
        for i, domain in enumerate(domains_to_check):
            # Жесткий лимит: пауза 0.35 сек гарантирует не более 3 запросов в секунду
            time.sleep(0.35) 
            
            traffic, requests_left = get_dataloom_traffic(domain, api_key)
            
            user_df.loc[i, 'Visits'] = str(traffic)
            
            progress_text = f"Проверен: {domain} | Визиты: {traffic} | Остаток реквестов: {requests_left} ({i+1}/{total_domains})"
            progress_fraction = min((i + 1) / total_domains, 1.0)
            progress_bar.progress(progress_fraction, text=progress_text)
            
        progress_bar.empty()
        st.success("Сбор данных завершен.")
        
        final_df = user_df.drop(columns=['Clean_Domain', '_original_order'])
        
        st.write("Предпросмотр результата:")
        st.dataframe(final_df.head(15))
        
        csv_buffer = io.BytesIO()
        final_df.to_csv(csv_buffer, index=False, encoding='utf-8')
        
        st.download_button(
            label="Скачать результат (CSV)",
            data=csv_buffer.getvalue(),
            file_name="dataloom_traffic_full.csv",
            mime="text/csv"
        )
