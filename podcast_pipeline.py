from airflow.decorators import dag, task
import pendulum
import requests
import xmltodict
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import os


@dag(
    dag_id='podcast_pipeline',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 12, 28),
    catchup=False,
)
def podcast_pipeline():

    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """,
        sqlite_conn_id="podcasts"
    )

    @task()
    def get_episodes():
        data = requests.get(
            "https://www.marketplace.org/feed/podcast/marketplace/")
        data_parse = xmltodict.parse(data.text)
        episodes = data_parse["rss"]["channel"]["item"]
        print("Number of episode:" + str(len(episodes)))

        return episodes

    podcast_ep = get_episodes()
    create_database.set_downstream(podcast_ep)

    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id='podcasts')
        query = hook.get_pandas_df("Select * from episodes;")
        new_episodes = []

        for episode in episodes:
            if episode['link'] not in query['link'].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"],
                                    episode["pubDate"], episode["description"], filename])

        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=[
                         "link", "title", "published", "description", "filename"])
        return new_episodes

    load_episodes(podcast_ep)

    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join(
                "/mnt/YOUR_PATH/download_podcasts", filename) #If you run the program on Linux, you must add the path /mnt /
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)

    download_episodes(podcast_ep)


run_etl = podcast_pipeline()
