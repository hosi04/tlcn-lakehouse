import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "olist-events")
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "100000"))

EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "10"))
PRODUCE_DELAY_MS = int(os.getenv("PRODUCE_DELAY_MS", "0"))

PREVIEW_EVENTS = int(os.getenv("PREVIEW_EVENTS", "0"))
PRODUCTS = [
    {"product_id": "1e9e8ef04dbcff4541ed26657ea517e5", "product_name": "Perfumery (225g)",           "category_id": "perfumaria",             "category_name": "Perfumery",             "price": 10.91,  "slug": "perfumery-1e9e8ef0"},
    {"product_id": "e3e020af31d4d89d2602272b315c3f6e", "product_name": "Health Beauty (75g)",        "category_id": "beleza_saude",           "category_name": "Health Beauty",         "price": 29.90,  "slug": "health-beauty-e3e020af"},
    {"product_id": "a1b71017a84f92fd8da4aeefba108a24", "product_name": "Computers Accessories (900g)", "category_id": "informatica_acessorios", "category_name": "Computers Accessories", "price": 69.90,  "slug": "computers-accessories-a1b71017"},
    {"product_id": "2548af3e6e77a690cf3eb6368e9ab61e", "product_name": "Furniture Decor (900g)",     "category_id": "moveis_decoracao",       "category_name": "Furniture Decor",       "price": 9.99,   "slug": "furniture-decor-2548af3e"},
    {"product_id": "14aa47b7fe5c25522b47b4b29c98dcb9", "product_name": "Bed Bath Table (1100g)",     "category_id": "cama_mesa_banho",        "category_name": "Bed Bath Table",        "price": 71.99,  "slug": "bed-bath-table-14aa47b7"},
    {"product_id": "96bd76ec8810374ed1b65e291975717f", "product_name": "Sports Leisure (154g)",      "category_id": "esporte_lazer",          "category_name": "Sports Leisure",        "price": 79.80,  "slug": "sports-leisure-96bd76ec"},
    {"product_id": "750cf819d127191920eda79a4b6fb479", "product_name": "Electronics (263g)",         "category_id": "eletronicos",            "category_name": "Electronics",           "price": 24.00,  "slug": "electronics-750cf819"},
    {"product_id": "cef67bcfe19066a932b7673e239eb23d", "product_name": "Baby (371g)",                "category_id": "bebes",                  "category_name": "Baby",                  "price": 112.30, "slug": "baby-cef67bcf"},
    {"product_id": "8c92109888e8cdf9d66dc7e463025574", "product_name": "Toys (600g)",                "category_id": "brinquedos",             "category_name": "Toys",                  "price": 82.90,  "slug": "toys-8c921098"},
    {"product_id": "9dc1a7de274444849c219cff195d0b71", "product_name": "Housewares (625g)",          "category_id": "utilidades_domesticas",  "category_name": "Housewares",            "price": 37.90,  "slug": "housewares-9dc1a7de"},
]
CATEGORIES = [
    {"category_id": "perfumaria",            "category_name": "Perfumery",             "slug": "perfumery"},
    {"category_id": "beleza_saude",          "category_name": "Health Beauty",         "slug": "health-beauty"},
    {"category_id": "informatica_acessorios", "category_name": "Computers Accessories", "slug": "computers-accessories"},
    {"category_id": "moveis_decoracao",      "category_name": "Furniture Decor",       "slug": "furniture-decor"},
    {"category_id": "cama_mesa_banho",       "category_name": "Bed Bath Table",        "slug": "bed-bath-table"},
    {"category_id": "esporte_lazer",         "category_name": "Sports Leisure",        "slug": "sports-leisure"},
    {"category_id": "eletronicos",           "category_name": "Electronics",           "slug": "electronics"},
    {"category_id": "bebes",                 "category_name": "Baby",                  "slug": "baby"},
    {"category_id": "brinquedos",            "category_name": "Toys",                  "slug": "toys"},
    {"category_id": "utilidades_domesticas", "category_name": "Housewares",            "slug": "housewares"},
]
SEARCH_KEYWORDS = [
    "perfumery", "perfumaria", "health beauty", "beleza saude",
    "computers accessories", "furniture decor", "bed bath table",
    "sports leisure", "electronics", "baby products", "brinquedos",
]
SOURCE_CHANNELS = ["direct", "organic_search", "paid_search", "social", "email", "referral"]
PLATFORMS       = ["web", "ios", "android"]
BASE_URL = "https://olist.com"
SITE_NAME = "Olist"