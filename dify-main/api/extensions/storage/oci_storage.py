from collections.abc import Generator
from contextlib import closing

import oci
from flask import Flask

from extensions.storage.base_storage import BaseStorage


class OCIStorage(BaseStorage):
    """Implementation for OCI storage.
    """
    def __init__(self, app: Flask):
        super().__init__(app)
        app_config = self.app.config
        self.bucket_name = app_config.get('OCI_BUCKET_NAME')
        
        config = {
            "user": app_config.get('OCI_USER_OCID'),
            "key_file": app_config.get('OCI_KEY_FILE'),
            "fingerprint": app_config.get('OCI_FINGERPRINT'),
            "tenancy": app_config.get('OCI_TENANCY_OCID'),
            "region": app_config.get('OCI_REGION')
        }

        self.client = oci.object_storage.ObjectStorageClient(config)
        self.namespace = self.client.get_namespace().data

    def save(self, filename, data):
        obj = oci.object_storage.models.PutObjectDetails()
        obj.put_object_body = data
        self.client.put_object(
            namespace_name=self.namespace,
            bucket_name=self.bucket_name,
            object_name=filename,
            put_object_body=data
        )

    def load_once(self, filename: str) -> bytes:
        try:
            with closing(self.client) as client:
                response = client.get_object(
                    namespace_name=self.namespace,
                    bucket_name=self.bucket_name,
                    object_name=filename
                )
                data = response.data.content
        except oci.exceptions.ServiceError as ex:
            if ex.status == 404:
                raise FileNotFoundError("File not found")
            else:
                raise
        return data

    def load_stream(self, filename: str) -> Generator:
        def generate(filename: str = filename) -> Generator:
            try:
                with closing(self.client) as client:
                    response = client.get_object(
                        namespace_name=self.namespace,
                        bucket_name=self.bucket_name,
                        object_name=filename
                    )
                    yield from response.data.raw.stream(1024)
            except oci.exceptions.ServiceError as ex:
                if ex.status == 404:
                    raise FileNotFoundError("File not found")
                else:
                    raise
        return generate()

    def download(self, filename, target_filepath):
        with closing(self.client) as client:
            response = client.get_object(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                object_name=filename
            )
            with open(target_filepath, 'wb') as file:
                for chunk in response.data.raw.stream(1024):
                    file.write(chunk)

    def exists(self, filename):
        with closing(self.client) as client:
            try:
                client.head_object(
                    namespace_name=self.namespace,
                    bucket_name=self.bucket_name,
                    object_name=filename
                )
                return True
            except oci.exceptions.ServiceError as ex:
                if ex.status == 404:
                    return False
                else:
                    raise

    def delete(self, filename):
        self.client.delete_object(
            namespace_name=self.namespace,
            bucket_name=self.bucket_name,
            object_name=filename
        )
