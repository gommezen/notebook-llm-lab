from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
pwd = os.getenv("NEO4J_PASS")

driver = GraphDatabase.driver(uri, auth=(user, pwd))
with driver.session() as s:
    print(s.run("RETURN 1 AS ok").single())
driver.close()
