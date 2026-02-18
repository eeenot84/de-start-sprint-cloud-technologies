## Scripts

- `create_topics.sh`: напоминалка про создание Kafka topics.
- `k8s_apply.sh`: применить k8s манифесты из `solution/k8s`.

Перед деплоем:
- Заполнить `k8s/00-configmap.yaml` (без паролей)
- Заполнить `k8s/01-secrets.yaml` (пароли/логины)
- Проставить образы в `k8s/10-dds-deployment.yaml` и `k8s/20-cdm-deployment.yaml`

