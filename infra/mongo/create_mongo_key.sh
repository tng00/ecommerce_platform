#!/bin/bash

# Директория, где будет создан keyfile (текущая директория, где запускается скрипт)
KEYFILE_DIR="." 
# Имя файла ключа
KEYFILE_NAME="mongo-keyfile"
KEYFILE_PATH="${KEYFILE_DIR}/${KEYFILE_NAME}"

# UID и GID для пользователя mongodb внутри Docker-контейнера (обычно 999)
MONGO_UID=999
MONGO_GID=999

echo "Creating MongoDB keyfile at ${KEYFILE_PATH} (in the current directory)..."

# 1. Директория уже существует, так как скрипт запускается из нее.
#    Просто проверяем, что мы можем писать в текущую директорию (косвенно проверится при создании файла).

# 2. Генерируем ключ
# openssl должен быть установлен в вашей системе
if ! command -v openssl &> /dev/null
then
    echo "Error: openssl command could not be found. Please install OpenSSL."
    exit 1
fi

openssl rand -base64 756 > "${KEYFILE_PATH}"
if [ $? -ne 0 ]; then
  echo "Error: Failed to generate keyfile using openssl."
  # Попытка удалить частично созданный файл, если генерация не удалась
  rm -f "${KEYFILE_PATH}" 
  exit 1
fi
echo "Generated keyfile content."

# 3. Устанавливаем владельца и права доступа
# ВНИМАНИЕ: Эти команды могут потребовать sudo, если вы не root
echo "Setting permissions for ${KEYFILE_PATH}..."

# Сначала попробуем без sudo, если текущий пользователь имеет права
chown "${MONGO_UID}:${MONGO_GID}" "${KEYFILE_PATH}"
CHOWN_EXIT_CODE=$?

chmod 400 "${KEYFILE_PATH}"
CHMOD_EXIT_CODE=$?

if [ ${CHOWN_EXIT_CODE} -ne 0 ] || [ ${CHMOD_EXIT_CODE} -ne 0 ]; then
  echo "Warning: Failed to set owner/permissions without sudo. Retrying with sudo..."
  # Если не удалось без sudo, пробуем с sudo
  sudo chown "${MONGO_UID}:${MONGO_GID}" "${KEYFILE_PATH}"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to set owner for ${KEYFILE_PATH} even with sudo. Please set manually: sudo chown ${MONGO_UID}:${MONGO_GID} ${KEYFILE_PATH}"
    # Не выходим, но chmod может также не сработать
  else
    echo "Owner set successfully with sudo."
  fi

  sudo chmod 400 "${KEYFILE_PATH}"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to set permissions for ${KEYFILE_PATH} even with sudo. Please set manually: sudo chmod 400 ${KEYFILE_PATH}"
    exit 1
  else
    echo "Permissions (400) set successfully with sudo."
  fi
else
  echo "Owner and permissions (400) set successfully without sudo."
fi

echo ""
echo "MongoDB keyfile created successfully at:"
echo "$(pwd)/${KEYFILE_NAME}" # Показываем абсолютный путь к созданному файлу
echo "Owner UID: ${MONGO_UID}, GID: ${MONGO_GID}"
echo "Permissions: 400 (r--------)"
echo ""
echo "Please ensure this path is correctly reflected in your docker-compose.infra.yml."
echo "For example, if docker-compose.infra.yml is in 'ecommerce_platform/infra/', the volume mount should be:"
echo "  volumes:"
echo "    - ./mongo/${KEYFILE_NAME}:/etc/mongo/mongo-keyfile:ro"

exit 0