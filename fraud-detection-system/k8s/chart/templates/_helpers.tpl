{{- define "platform.labels" -}}
app.kubernetes.io/part-of: data-platform
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* settings every lakehouse process (sinks, maintenance, catalog setup) shares */}}
{{- define "platform.lakehouseEnv" -}}
- name: ICEBERG_CATALOG_URI
  value: http://lakekeeper:8181/catalog
- name: ICEBERG_WAREHOUSE
  value: lakehouse
- name: S3_ENDPOINT
  value: http://seaweedfs:8333
- name: S3_REGION
  value: {{ .Values.storage.region }}
- name: S3_BUCKET
  value: {{ .Values.storage.bucket }}
- name: S3_ACCESS_KEY_ID
  valueFrom: {secretKeyRef: {name: platform-secrets, key: s3-access-key}}
- name: S3_SECRET_ACCESS_KEY
  valueFrom: {secretKeyRef: {name: platform-secrets, key: s3-secret-key}}
- name: SCHEMA_REGISTRY_URL
  value: http://schema-registry:8081
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ .Values.kafkaBootstrap }}
{{- end }}

{{/* an init container that waits until a URL answers 200 */}}
{{- define "platform.waitFor" -}}
- name: wait-for-{{ .name }}
  image: {{ .root.Values.images.lakehouse }}
  imagePullPolicy: {{ .root.Values.images.pullPolicy }}
  command:
    - python
    - -c
    - |
      import sys, time, urllib.request
      url = sys.argv[1]
      while True:
          try:
              urllib.request.urlopen(url, timeout=5)
              break
          except Exception as e:
              print(f"waiting for {url}: {e}", flush=True)
              time.sleep(3)
    - {{ .url | quote }}
{{- end }}
