from datetime import datetime, timedelta
from airflow import DAG
import time
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import requests

def fetch_weather(city, city_name, date):
    api_key = Variable.get("WEATHER_API_KEY")
    timestamp = int(time.mktime(datetime.strptime(date, "%Y-%m-%d").timetuple()))
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={city['lat']}&lon={city['lon']}&dt={timestamp}&appid={api_key}&units=metric"
    response = requests.get(url)
    data = response.json()
    print(data)
    weather_data = data['data'][0]

    temperature = weather_data.get('temp')
    humidity = weather_data.get('humidity')
    cloudiness = weather_data.get('clouds')
    wind_speed = weather_data.get('wind_speed')

    print(f"Weather for {city_name}: Temperature: {temperature}, Humidity: {humidity}, Cloudiness: {cloudiness}, Wind Speed: {wind_speed}")

   

def create_dag(city_name, city_coords, default_args):
    dag = DAG(
        f"weather_dag_{city_name}",
        default_args=default_args,
        description=f"Weather data extraction for {city_name}",
        schedule_interval="@daily",
	catchup=True
    )

    with dag:
        t1 = PythonOperator(
            task_id=f"fetch_weather_{city_name}",
            python_callable=fetch_weather,
            op_kwargs={'city': city_coords, 'city_name': city_name, 'date': "{{ ds }}"},
        )

    return dag


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 11, 17),
}

cities = {
    "Lviv": {"lat": 49.8397, "lon": 24.0297},
    "Kyiv": {"lat": 50.4501, "lon": 30.5234},
    "Kharkiv": {"lat": 49.9935, "lon": 36.2304},
    "Odesa": {"lat": 46.4825, "lon": 30.7233},
    "Zhmerynka": {"lat": 49.0391, "lon": 28.1086}
}

for city_name, city_coords in cities.items():
    globals()[f"dag_{city_name}"] = create_dag(city_name, city_coords, default_args)
