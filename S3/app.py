import asyncio  
import aiofiles
import os
from pathlib import Path
from contextlib import asynccontextmanager
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("s3_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("S3")

load_dotenv()

CONFIG = {
    "key_id": os.getenv("ACCESS_KEY_ID"),
    "secret": os.getenv("SECRET_ACCESS_KEY"),
    "endpoint": os.getenv("ENDPOINT"),
    "container": os.getenv("BUCKET"),
}

class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection

    async def send_file(self, local_source: str):
        file_ref = Path(local_source)
        target_name = file_ref.name
        async with self._connect() as remote:
            async with aiofiles.open(file_ref, "rb") as f:
                data = await f.read()
                await remote.put_object(
                    Bucket=self._bucket,
                    Key=target_name,
                    Body=data
                )
    
    async def fetch_file(self, remote_name: str, local_target: str):
        async with self._connect() as remote:
            response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
            body = await response["Body"].read()
            with open(local_target, "wb") as out:
                out.write(body)
    
    async def list_files(self, prefix: str = "") -> list[str]:
        #Возвращает список имён объектов в бакете.
        files = []
        async with self._connect() as remote:
            paginator = remote.get_paginator('list_objects_v2')
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    files.append(obj["Key"])
        return files

    async def file_exists(self, filename: str) -> bool:
        #Проверяет наличие файла с именем filename в бакете.
        async with self._connect() as remote:
            try:
                await remote.head_object(Bucket=self._bucket, Key=filename)
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return False

async def run_demo():
    storage = AsyncObjectStorage(
        key_id=CONFIG["key_id"],
        secret=CONFIG["secret"],
        endpoint=CONFIG["endpoint"],
        container=CONFIG["container"]
    )
    await storage.send_file("./new_file.txt")
    await storage.fetch_file("new_file.txt", "./files/new_file.txt")
    # Список файлов
    files = await storage.list_files()
    print("Files in bucket:", files)

    # Проверка существования файла
    exists = await storage.file_exists("new_file.txt")
    print(f"File 'new_file.txt' exists? {exists}")


if __name__ == "__main__":
    asyncio.run(run_demo())