import json
import random
import time
import os
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.client import V1ConfigMap

class QuantizeConfigMap:
    base_model_eval: bool = False
    base_model_eval_score: list = []
    quantization_progress: str = ""
    quantization_eval: bool = False
    quantization_eval_score: list = []


def create_or_update_configmap(data: QuantizeConfigMap, namespace: str, configmap_name: str):
    # Load Kubernetes config (inside the cluster)
    config.load_incluster_config()

    # Create ConfigMap object
    configmap = V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata={"name": configmap_name},
        data={k: json.dumps(v) for k, v in data.__dict__.items()}
    )

    # Kubernetes API client
    v1 = client.CoreV1Api()
    try:
        # Check if ConfigMap exists
        try:
            existing = v1.read_namespaced_config_map(configmap_name, namespace)
            # Update existing ConfigMap
            existing.data.update(configmap.data)
            v1.replace_namespaced_config_map(configmap_name, namespace, existing)
            print("Updated ConfigMap successfully.")
        except ApiException as e:
            if e.status == 404:
                # Create new ConfigMap if it doesn't exist
                v1.create_namespaced_config_map(namespace=namespace, body=configmap)
                print("Created ConfigMap successfully.")
            else:
                raise e
    except ApiException as e:
        print(f"Exception when handling ConfigMap: {e}")
        raise e

