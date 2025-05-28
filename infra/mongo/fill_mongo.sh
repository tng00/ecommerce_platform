#!/bin/bash

# Имя вашего MongoDB контейнера
MONGO_CONTAINER_NAME="mongo_ecommerce"
# Имя пользователя и пароль для MongoDB (если включена аутентификация)
MONGO_USER="mongoadmin"
MONGO_PASSWORD="mongopassword"
MONGO_AUTH_DB="admin"
# База данных, в которой находятся коллекции
DB_NAME="ecommerce_db"

# JavaScript код для вставки документов
MONGO_SCRIPT=$(cat <<EOF
use ${DB_NAME};

// 1. user_profiles_mongo
db.user_profiles_mongo.insertOne({
  userId: "user_mongo_auto_001",
  username: "mongo_auto_tester",
  email: "mongo_auto_tester@example.com",
  firstName: "MongoAuto",
  lastName: "TesterScript",
  preferences: {
    theme: "light",
    notifications: false
  },
  createdAt: new Date(),
  updatedAt: new Date()
});
print("Inserted 1 document into user_profiles_mongo");

// 2. seller_profiles_mongo
db.seller_profiles_mongo.insertOne({
  sellerId: "seller_mongo_auto_001",
  companyName: "Mongo Auto Goods Ltd.",
  contactEmail: "seller_auto@example.com",
  address: {
    street: "456 Auto Script Ave",
    city: "ScriptVille",
    zip: "67890",
    country: "JS"
  },
  isActive: true,
  rating: 4.2,
  createdAt: new Date(),
  updatedAt: new Date()
});
print("Inserted 1 document into seller_profiles_mongo");

// 3. product_details_mongo
db.product_details_mongo.insertOne({
  productId: "prod_mongo_auto_001",
  name: "Auto-Generated Widget",
  description: "A widget automatically inserted by a script.",
  sellerId: "seller_mongo_auto_001",
  category: "Automated Items",
  tags: ["mongodb", "widget", "scripted", "auto"],
  attributes: [
    { name: "Color", value: "Green" },
    { name: "Origin", value: "Script" }
  ],
  variants: [
    { sku: "AUTO-WIDGET-GRN-S", size: "Small", color: "Green", stock: 15 }
  ],
  price: {
    amount: 49.95,
    currency: "EUR"
  },
  averageRating: 4.0,
  imageUrl: "http://example.com/images/auto_widget.png",
  createdAt: new Date(),
  updatedAt: new Date()
});
print("Inserted 1 document into product_details_mongo");

// 4. reviews_mongo
db.reviews_mongo.insertOne({
  reviewId: "review_mongo_auto_001",
  productId: "prod_mongo_auto_001",
  userId: "user_mongo_auto_001",
  rating: 4,
  title: "Good for automation!",
  comment: "This auto-generated widget is quite functional.",
  isVerifiedPurchase: false,
  createdAt: new Date(),
  updatedAt: new Date()
});
print("Inserted 1 document into reviews_mongo");
EOF
)

# Проверка, запущен ли контейнер
if ! docker ps --filter "name=^/${MONGO_CONTAINER_NAME}$" --format "{{.Names}}" | grep -q "${MONGO_CONTAINER_NAME}"; then
  echo "Error: MongoDB container '${MONGO_CONTAINER_NAME}' is not running."
  exit 1
fi

echo "Attempting to insert data into MongoDB container '${MONGO_CONTAINER_NAME}'..."

# Выполнение скрипта в mongosh внутри Docker контейнера
docker exec -i "${MONGO_CONTAINER_NAME}" mongosh \
  --username "${MONGO_USER}" \
  --password "${MONGO_PASSWORD}" \
  --authenticationDatabase "${MONGO_AUTH_DB}" \
  --quiet \
  <<EOF
${MONGO_SCRIPT}
EOF

# Проверка кода завершения предыдущей команды
if [ $? -eq 0 ]; then
  echo "MongoDB data insertion script executed successfully."
else
  echo "Error executing MongoDB data insertion script. Check logs above or container logs."
  exit 1
fi

echo "To verify, connect to mongosh:"
echo "docker exec -it ${MONGO_CONTAINER_NAME} mongosh --username ${MONGO_USER} --password ${MONGO_PASSWORD} --authenticationDatabase ${MONGO_AUTH_DB}"
echo "Then run: use ${DB_NAME}; db.user_profiles_mongo.find(); // (and for other collections)"