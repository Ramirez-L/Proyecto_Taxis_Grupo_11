API_KEY_LIST= [
    "VBF8WCN29MQP5BNCBQ5QG5U7E",
    "EPVYJQV9TTNYYN52QNYRLEL58",
    "P3KPQJVC4HWS6HZ9A8KR94TV3",
    "E3DLF4LEV66P6WCEDPK2HTA5Z",
    "99LZHCW823HQ5P5Y44XPVR2F5",
    "WPDFT7FX9KGMSAZG9YMQMRSLB",
]

HOST="postgres"
DBNAME="taxisdb"
USER="airflow"
PASS="airflow"

DATABASE_LOCATION=f'postgresql+psycopg2://{USER}:{PASS}@{HOST}:5432/{DBNAME}'
BASE_URL_TAXIS="https://d37ci6vzurychx.cloudfront.net/trip-data/"
