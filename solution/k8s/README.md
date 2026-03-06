## Kubernetes manifests (templates)

Манифесты в этой папке — **шаблоны**. Секреты (пароли/логины) сюда не записываются.

### Быстрый старт

1) Создайте namespace (по желанию):

```bash
kubectl create namespace sprint9 --dry-run=client -o yaml | kubectl apply -f -
```

2) Создайте `Secret` c переменными окружения. **Не коммитьте** значения.

Вариант A (рекомендуется): создать секрет командой:

```bash
kubectl -n sprint9 create secret generic sprint9-dwh-secrets \
  --from-literal=KAFKA_CONSUMER_USERNAME="..." \
  --from-literal=KAFKA_CONSUMER_PASSWORD="..." \
  --from-literal=PG_WAREHOUSE_PASSWORD="..."
```

Вариант B: используйте `01-secrets.yaml` как **шаблон** (значения храните локально).

3) Примените манифесты:

```bash
kubectl -n sprint9 apply -f 00-configmap.yaml
kubectl -n sprint9 apply -f 01-secrets.yaml
kubectl -n sprint9 apply -f 10-dds-deployment.yaml
kubectl -n sprint9 apply -f 20-cdm-deployment.yaml
```

