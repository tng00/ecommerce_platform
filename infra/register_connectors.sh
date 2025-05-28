#!/bin/bash

# Адрес Kafka Connect REST API
CONNECT_URL="http://localhost:8083/connectors"

# Пути к файлам конфигурации коннекторов (относительно текущей директории, т.е. infra/)
POSTGRES_CONFIG_FILE="./kafka-connect/connectors/register-postgres.json"
MONGO_CONFIG_FILE="./kafka-connect/connectors/register-mongo.json"

# Имена коннекторов (должны совпадать с полем "name" в JSON файлах)
POSTGRES_CONNECTOR_NAME="pg-ecommerce-connector"
MONGO_CONNECTOR_NAME="mongo-ecommerce-connector"

echo "Attempting to register Debezium connectors..."
echo "Kafka Connect URL: ${CONNECT_URL}"
echo ""

# Функция для регистрации или обновления коннектора
register_or_update_connector() {
  local connector_name=$1
  local config_file=$2
  local connector_type_log=$3 # "PostgreSQL" или "MongoDB" для логирования

  echo "-----------------------------------------------------"
  echo "Processing ${connector_type_log} connector: ${connector_name}"
  echo "Config file: ${config_file}"

  if [ ! -f "${config_file}" ]; then
    echo "ERROR: Configuration file ${config_file} not found!"
    echo "Skipping ${connector_type_log} connector."
    echo "-----------------------------------------------------"
    return 1
  fi

  # Проверяем, существует ли коннектор
  STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/${connector_name}/status")

  if [ "${STATUS_CODE}" -eq 200 ]; then
    echo "Connector ${connector_name} already exists. Attempting to update configuration (PUT request)..."
    RESPONSE_CODE=$(curl -s -X PUT -H "Content-Type: application/json" \
        -d @"${config_file}" \
        -w "%{http_code}" -o >(cat >&2) \
        "${CONNECT_URL}/${connector_name}/config")
    # Вывод ответа сервера будет в stderr из-за `>(cat >&2)`
    # Это чтобы `RESPONSE_CODE` получил только код.
    # Если нужна только реакция на код, можно -o /dev/null
  else
    echo "Connector ${connector_name} does not exist or an error occurred (status: ${STATUS_CODE}). Attempting to create (POST request)..."
    RESPONSE_CODE=$(curl -s -X POST -H "Content-Type: application/json" \
        -d @"${config_file}" \
        -w "%{http_code}" -o >(cat >&2) \
        "${CONNECT_URL}")
  fi
  
  echo "" # Пустая строка после вывода ответа curl
  if [[ "${RESPONSE_CODE}" -ge 200 && "${RESPONSE_CODE}" -lt 300 ]]; then
    echo "SUCCESS: ${connector_type_log} connector '${connector_name}' request sent successfully (HTTP ${RESPONSE_CODE})."
    echo "Checking status for ${connector_name}..."
    sleep 5 # Даем время на запуск
    curl -s "${CONNECT_URL}/${connector_name}/status" | jq . # Используем jq для красивого вывода, если установлен
    if ! command -v jq &> /dev/null; then
        echo "(jq not found, raw JSON output above)"
    fi
  else
    echo "ERROR: Failed to register/update ${connector_type_log} connector '${connector_name}' (HTTP ${RESPONSE_CODE})."
    echo "Please check the output above and Kafka Connect logs."
  fi
  echo "-----------------------------------------------------"
  echo ""
}

# Регистрируем/обновляем PostgreSQL коннектор
register_or_update_connector "${POSTGRES_CONNECTOR_NAME}" "${POSTGRES_CONFIG_FILE}" "PostgreSQL"

# Регистрируем/обновляем MongoDB коннектор
register_or_update_connector "${MONGO_CONNECTOR_NAME}" "${MONGO_CONFIG_FILE}" "MongoDB"

echo "All specified connector registrations attempted."
echo "Check Kafka Connect logs and Kafdrop for topics."