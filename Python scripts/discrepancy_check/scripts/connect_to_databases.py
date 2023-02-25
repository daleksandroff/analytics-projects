
# This script provides the functions for create connections to databases (MongoDB, Redshift) and S3.

## Import modules:
from pymongo import MongoClient
from sqlalchemy import create_engine
import os

class DbConnections(object):
    r"""Class that perform connections to MongoDB, AWS Redshift and AWS S3.
    
    Args:
    - odbc_file_path: odbc config file with credentials
    
    Methods:
    - connect_to_mongo: method for connection to MongoDB. Returns dict with Mongo connection and DB objects.
    - connect_to_redshift(echo=True): method for connection to AWS Redshift. Returns dict with Redshift connection and Engine objects. Has parameter 'echo', when it = True then print connection logs.
    - connect_to_s3: method for connection to AWS S3. Returns dict with S3 session resource and credentials.
    - close_connections: method for closing connections to Mongo and Redshift.
    
    Usage:
    >>> from reload_main_script import reload_data_main as r
    >>> import getpass
    >>> odbc_file_path = '/users/' + getpass.getuser() + '/.odbc.ini'
    >>> connections = r.db_connections(odbc_file_path=odbc_file_path)
    >>> connections.connect_to_mongo()
    >>> connections.connect_to_redshift()
    >>> connections.connect_to_s3()
    >>> ...
    >>> connections.close_connections()
    
    """
    
    def __init__(self,
                 odbc_file_path=None):
        self.odbc_file_path = odbc_file_path
    
    def connect_to_mongo(self):
        print('Connecting to Mongo...')
        conn_link = os.environ['MONGO_URL']
        mongo_conn = MongoClient(conn_link)
        mongo_db = mongo_conn['analytics_prod']
        print('Connected to Mongo.')
        
        self.mongo = {'conn': mongo_conn, 'db': mongo_db}
        return
    
    def connect_to_redshift(self, echo=False):
        print('Connecting to Redshift...')
        conn_link = os.environ['REDSHIFT_URL']
        redshift_engine = create_engine(conn_link, echo=echo)
        redshift_conn = redshift_engine.connect()
        print('Connected to Redshift.')
        
        self.redshift = {'conn': redshift_conn, 'engine': redshift_engine}
        return
    
    def close_connections(self):
        print('Closing all DB connections...')
        if hasattr(self, 'redshift'):
            self.redshift['conn'].close()
        if hasattr(self, 'mongo'):
            self.mongo['conn'].close()
        # s3 - no need to close connection ( https://forums.aws.amazon.com/thread.jspa?threadID=265894 )
        print('All DB connections are closed.')
        return