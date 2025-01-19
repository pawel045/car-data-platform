from dotenv import load_dotenv
import os

load_dotenv()

db_host = os.getenv('DB_HOST')
print(db_host)

