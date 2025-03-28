from minio import Minio
from minio.error import S3Error
from kubernetes import client, config
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import base64
import json

class MinioClientWrapper:
    def __init__(self, endpoint=None, access_key=None, secret_key=None, secret_name=None, namespace=None, keyvault_name=None, secure=True, cert_check=True):
        """Initialize the Minio client wrapper with given credentials or fall back to Kubernetes secrets and Azure Key Vault."""
        if not endpoint or not access_key or not secret_key:
            endpoint, access_key, secret_key = self._get_k8s_secret(secret_name, namespace) if secret_name and namespace else (None, None, None)
        
        if not endpoint or not access_key or not secret_key:
            endpoint, access_key, secret_key = self._get_azure_keyvault_secret(keyvault_name) if keyvault_name else (None, None, None)
        
        if not endpoint or not access_key or not secret_key:
            raise ValueError("MinIO credentials could not be found in provided arguments, Kubernetes secrets, or Azure Key Vault.")
        
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure, cert_check=cert_check)
    
    def _get_k8s_secret(self, secret_name, namespace):
        """Retrieve MinIO credentials from a Kubernetes secret."""
        try:
            config.load_incluster_config()
            v1 = client.CoreV1Api()
            secret = v1.read_namespaced_secret(secret_name, namespace)
            secret_data = json.loads(base64.b64decode(secret.data['minio']).decode('utf-8'))
            return secret_data.get('host'), secret_data.get('id'), secret_data.get('secret')
        except Exception:
            return None, None, None
    
    def _get_azure_keyvault_secret(self, keyvault_name):
        """Retrieve MinIO credentials from Azure Key Vault."""
        try:
            credential = DefaultAzureCredential()
            vault_url = f"https://{keyvault_name}.vault.azure.net"
            client = SecretClient(vault_url=vault_url, credential=credential)
            secret_data = json.loads(client.get_secret("minio-credentials").value)
            return secret_data.get('host'), secret_data.get('id'), secret_data.get('secret')
        except Exception:
            return None, None, None
    
    def bucket_exists(self, bucket_name):
        """Check if a bucket exists."""
        return self.client.bucket_exists(bucket_name)
    
    def create_bucket(self, bucket_name):
        """Create a bucket if it doesn't exist."""
        if not self.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
    
    def upload_file(self, bucket_name, object_name, file_path):
        """Upload a file to a bucket."""
        self.client.fput_object(bucket_name, object_name, file_path)
    
    def download_file(self, bucket_name, object_name, file_path):
        """Download a file from a bucket."""
        self.client.fget_object(bucket_name, object_name, file_path)
    
    def list_objects(self, bucket_name, prefix=None):
        """List objects in a bucket."""
        return [obj.object_name for obj in self.client.list_objects(bucket_name, prefix=prefix)]
    
    def delete_object(self, bucket_name, object_name):
        """Delete an object from a bucket."""
        self.client.remove_object(bucket_name, object_name)

    def delete_bucket(self, bucket_name):
        """Delete a bucket if empty."""
        self.client.remove_bucket(bucket_name)
    
# Example usage
if __name__ == "__main__":
    # Example with direct credentials
    minio_client = MinioClientWrapper(endpoint="play.min.io", access_key="your-access-key", secret_key="your-secret-key", secure=True, cert_check=True)
    minio_client.create_bucket("my-bucket")
    minio_client.upload_file("my-bucket", "example.txt", "./example.txt")
    print(minio_client.list_objects("my-bucket"))
    
    # Example with Kubernetes secret and Azure Key Vault fallback
    minio_client_fallback = MinioClientWrapper(secret_name="minio-secret", namespace="default", keyvault_name="my-keyvault", secure=True, cert_check=True)
    minio_client_fallback.create_bucket("fallback-bucket")
    minio_client_fallback.upload_file("fallback-bucket", "example.txt", "./example.txt")
    print(minio_client_fallback.list_objects("fallback-bucket"))
    
    # Example with only Kubernetes secret
    minio_client_k8s = MinioClientWrapper(secret_name="minio-secret", namespace="default", secure=True, cert_check=False)
    minio_client_k8s.create_bucket("k8s-bucket")
    minio_client_k8s.upload_file("k8s-bucket", "example.txt", "./example.txt")
    print(minio_client_k8s.list_objects("k8s-bucket"))
    
    # Example with only Azure Key Vault
    minio_client_azure = MinioClientWrapper(keyvault_name="my-keyvault", secure=False, cert_check=True)
    minio_client_azure.create_bucket("azure-bucket")
    minio_client_azure.upload_file("azure-bucket", "example.txt", "./example.txt")
    print(minio_client_azure.list_objects("azure-bucket"))
