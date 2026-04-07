import streamlit as st
import pandas as pd
import requests
import re
import io
import time
import json

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
        
        if response.status_code == 200:
            data = response.json()
            
            # Разные провайдеры называют поле с трафиком по-разному.
            # Проверяем самые популярные варианты.
            if 'total_visits' in data:
                return data['total_visits']
            elif 'visits' in data:
                return data['visits']
            elif 'Traffic' in data:
                return data['Traffic']
            else:
                # Если структура ответа неизвестна, выводим весь JSON для отладки
                st.write(f"Ответ для {domain}:", data)
                return "См. лог на экране"
                
        elif response.status_code == 429:
            return "Лимит запросов исчерпан"
        elif response.status_code == 401 or response.status_code == 403:
            return "Ошибка ключа API"
        else:
            return f"Ошибка API: {response.status_code}"
            
    except Exception as e:
        return "Ошибка соединения"

st.set_page_config(page_title="DataLoom Traffic Checker", layout="wide")
st.title("Сбор трафика через DataLoom (RapidAPI)")

st.sidebar.header("Настройки доступа")
api_key = st.sidebar.text_input("Введи твой x-rapidapi-key", type="password")
api_limit = st.sidebar.number_input("Лимит сайтов для проверки", min_value=1, max_value=500, value=5)

uploaded_file = st.file_uploader("Загрузите ваш CSV файл со списком сайтов", type=["csv"])

if uploaded_file:
    if not api_key:
        st.warning("Пожалуйста, введите ключ API в боковом меню слева.")
        st.stop()
        
    with st.spinner('Чтение файла...'):
        user_df = pd.read_csv(uploaded_file)
        if user_df.empty:
            st.error("Файл пуст.")
            st.stop()
            
        user_df['_original_order'] = range(len(user_df))
        site_column = user_df.columns[0]
        user_df['Clean_Domain'] = user_df[site_column].apply(clean_url)
        user_df['Visits'] = '-'
        
    if st.button("Начать сбор трафика", type="primary"):
        st.info(f"Запускаем проверку для первых {api_limit} сайтов...")
        
        domains_to_check = user_df['Clean_Domain'].head(api_limit).tolist()
        progress_bar = st.progress(0, text="Опрашиваем API...")
        
        for i, domain in enumerate(domains_to_check):
            # Делаем паузу, чтобы бесплатный тариф не заблокировал за спам запросами
            time.sleep(1.5) 
            
            traffic = get_dataloom_traffic(domain, api_key)
            user_df.loc[i, 'Visits'] = traffic
            
            progress_bar.progress((i + 1) / api_limit, text=f"Проверен: {domain} | Результат: {traffic}")
            
        progress_bar.empty()
        st.success("Сбор данных завершен!")
        
        final_df = user_df.drop(columns=['Clean_Domain', '_original_order'])
        
        st.write("Предпросмотр результата:")
        st.dataframe(final_df.head(api_limit))
        
        csv_buffer = io.BytesIO()
        final_df.to_csv(csv_buffer, index=False, encoding='utf-8')
        
        st.download_button(
            label="Скачать результат (CSV)",
            data=csv_buffer.getvalue(),
            file_name="dataloom_traffic.csv",
            mime="text/csv"
        )
