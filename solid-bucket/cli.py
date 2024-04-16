from bucket import Bucket

import asyncio
import os
from time import perf_counter
from multiprocessing import Pool
import json

import boto3
from boto3.exceptions import Boto3Error
import click


def download(bucket_name: str, src_obj: str, dest_path: str) -> None:
    """
    Юнит задачи для параллельной загрузки файлов.

    :param bucket_name: Имя бакета
    :param src_obj: Исходный объект для выгузки
    :param dest_path: Конечный путь для сохранения
    """
    global client
    try:
        client.download_file(bucket_name, src_obj, dest_path)
    except Boto3Error:
        print(f"Файл /{src_obj} не найден")


def download_handler(bucket_name: str, prefix: str, cpus: int, local_dir: str) -> int:
    """
    Обработчик параллельной загрузки файлов.

    :param bucket_name: Имя бакета
    :param prefix: Префикс для загрузки файлов
    :param cpus: Количество процессов используемых для параллельной загрузки
    :param local_dir: Папка для сохранения файлов
    """
    global client
    pg = client.get_paginator("list_objects_v2")
    pages = pg.paginate(Bucket=bucket_name, Prefix=prefix)
    pool = Pool(cpus)
    mp_data = []
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                src_obj = obj["Key"]
                dest_path = local_dir + "" + src_obj
                mp_data.append((bucket_name, src_obj, dest_path))
                os.path.dirname(dest_path) and os.makedirs(
                    os.path.dirname(dest_path), exist_ok=True
                )
    pool.starmap(download, mp_data)
    return len(mp_data)


@click.command(help="Утилита загрузки файлов с хранилища объектов AWS S3")
@click.help_option(help="Показать это сообщение и завершить работу")
@click.version_option(
    "0.1.0", prog_name="solid-bucket", help="Показать версию утилиты и завершить работу"
)
@click.argument("bucket", type=click.STRING)
@click.option(
    "-p", "--prefix", type=click.STRING, help="Префикс объекта для скачивания"
)
@click.option("-k", "--key", type=click.STRING, help="Ключ объекта для скачивания")
@click.option(
    "-cp",
    "--cpus",
    type=click.INT,
    help="Число ядер используемых в параллельной загрузке",
)
@click.option("-cr", "--coroutines", is_flag=True, help="Асинхронный способ загрузки")
@click.option(
    "-ld",
    "--localdir",
    type=click.STRING,
    default="download/",
    help="Путь к папке для загрузки",
)
@click.option("-cf", "--config", type=click.STRING, help="Путь к файлу конфигурации")
@click.option(
    "-ak", "--accesskey", type=click.STRING, help="Указать Access Key в ручную"
)
@click.option(
    "-sk", "--secretkey", type=click.STRING, help="Указать Secret Key в ручную"
)
@click.option(
    "-e", "--endpoint", type=click.STRING, help="Указать endpoint url в ручную"
)
@click.option("-t", "--test", is_flag=True, help="Протестировать работу утилиты")
def cli(
    bucket,
    prefix,
    key,
    cpus,
    coroutines,
    localdir,
    config,
    accesskey,
    secretkey,
    endpoint,
    test,
):
    """
    Обработчик параметров переданных утилите.
    """
    if config:
        with open(config) as credentials:
            data = json.load(credentials)
            Bucket.prepare(
                data.get("accessKey", None),
                data.get("secretKey", None),
                data.get("url", None),
            )
    else:
        if accesskey and secretkey:
            Bucket.prepare(accesskey, secretkey, endpoint if endpoint else None)
    if test:
        bkt = Bucket("test")
        loop = asyncio.get_event_loop()
        print("Создание тестовых файлов в хранилище")
        loop.run_until_complete(bkt.test_setup())
        print("Создание тестовый файлов в бакет test, по префиксу test, завершено")
    if cpus:
        global client
        client = boto3.client(
            "s3",
            endpoint_url=Bucket._ENDPOINT_URL,
            aws_access_key_id=Bucket._AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Bucket._AWS_SECRET_ACCESS_KEY,
            aws_session_token=None,
        )

        start_time = perf_counter()
        print(f"Загрузка в {localdir}...")
        print(f"Загружено {download_handler(bucket, prefix, cpus, localdir)} файлов")
        end_time = perf_counter()
        print(
            f"Время параллельной загрузки файлов {round(end_time - start_time, 2)} сек."
        )
    else:
        if coroutines:
            bkt = Bucket(bucket)
            loop = asyncio.get_event_loop()
            start_time = perf_counter()
            print(f"Загрузка в {localdir}...")
            if prefix:
                loop.run_until_complete(bkt.get_files_by_prefix(prefix, localdir))
            else:
                if key:
                    loop.run_until_complete(bkt.get_files_by_key(key, localdir))
            end_time = perf_counter()
            print(
                f"Загрузка завершена!\nВремя асинхронной загрузки файлов {round(end_time - start_time, 2)} сек."
            )
