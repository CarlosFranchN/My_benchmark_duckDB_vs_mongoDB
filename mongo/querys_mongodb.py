from mock_mongodb import CONN_STRING, with_mongo_db
import pandas as pd
@with_mongo_db(db_name="mongo_db" , collection_name="users")
def select_users(collection):
    cursor = collection.find({}).limit(5)
    df = pd.DataFrame(list(cursor))
    print(df)

if __name__ == "__main__":
    select_users()