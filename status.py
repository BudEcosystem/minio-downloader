import random
import time
import os
from kubernetes import client, config

class QuantizeConfigMap:
    base_model_eval: bool = False
    base_model_eval_score: list = []
    quantization_progress: str = ""
    quantization_eval: bool = False
    quantization_eval_score: list = []


def create_or_update_configmap(data: QuantizeConfigMap, namespace: str, configmap_name: str, max_retries: int = 3):
    """Create or update a Kubernetes ConfigMap with intermediate results."""
    # Load Kubernetes config (inside the cluster)
    config.load_incluster_config()

    # Kubernetes API client
    v1 = client.CoreV1Api()
    
    for attempt in range(max_retries):
        try:
            # Check if ConfigMap exists
            existing = v1.read_namespaced_config_map(configmap_name, namespace)
            
            # Update existing ConfigMap
            existing.data.update(data)
            v1.replace_namespaced_config_map(configmap_name, namespace, existing)
            print("Updated ConfigMap successfully.")
            return  # Success, exit the function
        
        except client.exceptions.ApiException as e:
            if e.status == 404:
                # Create new ConfigMap if it doesn't exist
                try:
                    cm = client.V1ConfigMap(
                        metadata=client.V1ObjectMeta(name=configmap_name),
                        data=data
                    )
                    v1.create_namespaced_config_map(namespace=namespace, body=cm)
                    print("Created new ConfigMap.")
                    return  # Success, exit the function
                except client.exceptions.ApiException as create_err:
                    if create_err.status == 409:
                        # ConfigMap was created by another thread, retry the update
                        if attempt < max_retries - 1:
                            time.sleep(0.1 * (2 ** attempt))  # Exponential backoff
                            continue
                    print(f"Error creating ConfigMap: {create_err}")
                    return
            elif e.status == 409:
                # Conflict error - ConfigMap was modified, retry with exponential backoff
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (2 ** attempt))  # Exponential backoff: 0.1s, 0.2s, 0.4s
                    continue
                else:
                    print(f"Error updating ConfigMap after {max_retries} retries: {e}")
                    return
            else:
                print(f"Error updating ConfigMap: {e}")
                return


def update_status(configmap: dict):
    namespace = os.environ.get("NAMESPACE")
    configmap_name = os.environ.get("CONFIGMAP_NAME")
    use_kubernetes = os.environ.get("USE_KUBERNETES")
    print(f"namespace: {namespace}")
    print(f"configmap_name: {configmap_name}")
    print(f"use_kubernetes: {use_kubernetes}")
    if use_kubernetes == "True":
        create_or_update_configmap(configmap, namespace, configmap_name)
    else:
        print(f"configmap: {configmap.__dict__}")