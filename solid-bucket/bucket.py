import os
from random import randint
from contextlib import AsyncExitStack

from aiobotocore.session import get_session
import aiofiles


class Bucket:
    """
    Класс для хранения информации о бакете и загрузки файлов с использованием корутин.
    """

    _AWS_ACCESS_KEY_ID = None
    _AWS_SECRET_ACCESS_KEY = None
    _ENDPOINT_URL = None

    def __init__(self, bucket_name: str):
        # Проверка на то, что утилита инициализирована с корректными ключами
        if not Bucket._AWS_ACCESS_KEY_ID or not Bucket._AWS_SECRET_ACCESS_KEY:
            raise TypeError(
                "Access key ID и Secret access key обязателньо должны быть указаны при вызове утилиты. "
                "Они станут атрибутами класса."
            )
        self.session = get_session()
        self.bucket_name = bucket_name

    @classmethod
    def prepare(
        cls,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        endpoint_url: str | None,
    ) -> None:
        """
        Подготовка бакета, добавление ключей в переменные класса.

        :param aws_access_key_id: Access Key
        :param aws_secret_access_key: Secret Key
        :param endpoint_url: Опциональный путь к конечной точке
        """
        cls._AWS_ACCESS_KEY_ID = aws_access_key_id
        cls._AWS_SECRET_ACCESS_KEY = aws_secret_access_key
        cls._ENDPOINT_URL = endpoint_url

    async def _create_client(self, exit_stack: AsyncExitStack):
        """
        Создание клиента для соединения с хранилищем по переданным ключам.

        :param exit_stack: Асинхронный контекстный менеджер
        """
        client = await exit_stack.enter_async_context(
            self.session.create_client(
                "s3",
                endpoint_url=Bucket._ENDPOINT_URL,
                aws_secret_access_key=Bucket._AWS_SECRET_ACCESS_KEY,
                aws_access_key_id=Bucket._AWS_ACCESS_KEY_ID,
                aws_session_token=None,
            )
        )
        return client

    async def _write_file(self, dest_path: str, data: bytes) -> None:
        """
        Запись файла с хранилища.

        :param dest_path: Путь к конечному файлу для загрузки
        :param data: Данные для записи в файл
        """
        os.path.dirname(dest_path) and os.makedirs(
            os.path.dirname(dest_path), exist_ok=True
        )
        async with aiofiles.open(dest_path, "wb") as f:
            await f.write(data)

    async def get_files_by_prefix(self, prefix: str, local_dir: str) -> None:
        """
        Загрузка файлов по префиксу.

        :param prefix: Префикс
        :param local_dir: Папка для загрузки
        """
        async with AsyncExitStack() as exit_stack:
            client = await self._create_client(exit_stack)
            paginator = client.get_paginator("list_objects")
            async for result in paginator.paginate(
                Bucket=self.bucket_name, Prefix=prefix
            ):
                for c in result.get("Contents", []):
                    resp = await client.get_object(
                        Bucket=self.bucket_name, Key=c["Key"]
                    )
                    async with resp["Body"] as stream:
                        data = await stream.read()
                        await self._write_file(local_dir + c["Key"], data)

    async def get_files_by_key(self, key: str, local_dir: str) -> None:
        """
        Загрузка файла по ключу.

        :param key: Ключ
        :param local_dir: Папка для загрузки
        """
        async with AsyncExitStack() as exit_stack:
            client = await self._create_client(exit_stack)
            resp = await client.get_object(Bucket=self.bucket_name, Key=key)
            async with resp["Body"] as stream:
                data = await stream.read()
                await self._write_file(local_dir + key, data)

    async def test_setup(self) -> None:
        """
        Функция создающая тестовые файлы для загрузки.
        """
        filename = "dummy"
        folder = "test"
        key = "{}/{}".format(folder, filename)
        async with AsyncExitStack() as exit_stack:
            client = await self._create_client(exit_stack)
            data = b"\x01" * 1024 * randint(100, 5000)
            for n in range(1, 10000):
                await client.put_object(
                    Bucket=self.bucket_name, Key=key + str(n), Body=data
                )

    async def test_teardown(self) -> None:
        """
        Функция удаляющая тестовые файлы.
        """
        filename = "dummy"
        folder = "test"
        key = "{}/{}".format(folder, filename)
        async with AsyncExitStack() as exit_stack:
            client = await self._create_client(exit_stack)
            for n in range(1, 10000):
                await client.delete_object(Bucket=self.bucket_name, Key=key + str(n))
