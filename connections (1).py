# -*- coding: utf-8 -*-
"""
Created on Feb 03 2022

@author: A00007371
"""
import getpass
import os
import pyodbc
from databricks import sql


# =============================================================================
# Connection to Dremio (odbc driver)
# =============================================================================
def dbx_connection(
    user,
    server_hostname=None,
    http_path=None,
    token_env=None,
):
    """Create a Databricks SQL connection.

    Parameters
    ----------
    user : str
        User initials used to resolve the environment variable that stores
        the access token (``DBX_TOKEN_<USER>`` by default).
    server_hostname : str, optional
        Databricks server hostname. Defaults to the value of the
        ``DBX_SERVER_HOSTNAME`` environment variable.
    http_path : str, optional
        HTTP path to the SQL warehouse. Defaults to ``DBX_HTTP_PATH``.
    token_env : str, optional
        Name of the environment variable containing the access token. When
        omitted, ``DBX_TOKEN_<USER>`` is used.

    Returns
    -------
    databricks.sql.connect.Connection
        Connection object ready for executing SQL queries.
    """
    server_hostname = server_hostname or os.environ.get("DBX_SERVER_HOSTNAME")
    http_path = http_path or os.environ.get("DBX_HTTP_PATH")
    env_name = token_env or f"DBX_TOKEN_{user.upper()}"
    access_token = os.environ.get(env_name)
    if not all([server_hostname, http_path, access_token]):
        raise ValueError("Missing Databricks connection configuration.")
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    )

def dbx_connection_dev(
    user,
    server_hostname=None,
    http_path=None,
    token_env=None,
):
    """Create a Databricks SQL connection to the development warehouse.

    Parameters are equivalent to :func:`dbx_connection` but use different
    environment variable names (``DBX_DEV_*``)."""
    server_hostname = server_hostname or os.environ.get("DBX_DEV_SERVER_HOSTNAME")
    http_path = http_path or os.environ.get("DBX_DEV_HTTP_PATH")
    env_name = token_env or f"DBX_DEV_TOKEN_{user.upper()}"
    access_token = os.environ.get(env_name)
    if not all([server_hostname, http_path, access_token]):
        raise ValueError("Missing Databricks dev connection configuration.")
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    )

def dremio_connection(email, database_type = 'prod'):
    """This function establishes a connection to Dremio.
    Args:
        email (str): Company email used to access Dremio.
        database_type (str): choose between "prod" (default) and "dev" database
    Returns:
        conn (connection object): Dremio connection object to be used in SQL queries.
    """
    uid = email
    pwd = getpass.getpass(prompt='Password for Dremio: ')
    if database_type == 'prod':
        host = os.environ.get("DREMIO_PROD_HOST", "dremio.1corp.org")
    elif database_type == 'dev':
        host = os.environ.get("DREMIO_DEV_HOST", "dremio-dev.1corp.org")
    else: 
        print('Database not existent, please enter a valid argument.')

    port = 31010
    driver = "Dremio Connector"
    try:
        conn = pyodbc.connect(
            "Driver={};ConnectionType=Direct;HOST={};PORT={};AuthenticationType=Plain;UID={};PWD={};SSL=1;DisableCertificateVerification=1".format(
                driver, host, port, uid, pwd
            ),
            autocommit=True,
        )
        return conn
    except:
        print('Connection Error')



# Azure connection
def susie_connection(email, database_type = 'prod'):    
    """This function establishes a connection to SuSIE Cloud Database.
    Args:
        email (str): Company email.
        database_type (str): choose between "prod" (default) and "dev" database
    Returns:
        conn (connection object): SQL Server connection object to be used in SQL queries.
    """
    username = email
    if database_type == 'prod':
        server = os.environ.get('SUSIE_PROD_SERVER', 'paz1eu-s141-sust-csg01-sql01.database.windows.net')
    elif database_type == 'dev':
        server = os.environ.get('SUSIE_DEV_SERVER', 'daz1eu-s139-sust-csg01-sql01.database.windows.net')
    elif database_type == 'test':
        server = os.environ.get('SUSIE_TEST_SERVER', 'taz1eu-s140-sust-csg01-sql01.database.windows.net')
    else: 
        print('Database not existent, please enter a valid argument.')
    
    database = 'sustainability'
    Authentication='ActiveDirectoryInteractive'
    driver= '{ODBC Driver 17 for SQL Server}'
    try:
        conn = pyodbc.connect('DRIVER='+driver+
                              ';SERVER='+server+
                              ';PORT=1433;DATABASE='+database+
                              ';UID='+username+
                              ';AUTHENTICATION='+Authentication
                              )
        return(conn)
    except: 
        print('Connection Error')
        
def genie_connection():
    """Establish a connection to the Genie SQL Server.

    Connection parameters are read from the environment variables
    ``GENIE_USERNAME``, ``GENIE_PASSWORD``, ``GENIE_SERVER`` and
    ``GENIE_DATABASE`` (defaults to ``master``).
    """
    username = os.environ.get("GENIE_USERNAME")
    password = os.environ.get("GENIE_PASSWORD")
    server = os.environ.get("GENIE_SERVER")
    database = os.environ.get("GENIE_DATABASE", "master")
    driver = '{SQL Server}'

    if not all([username, password, server]):
        raise ValueError("Missing Genie connection configuration.")
    try:
        conn = pyodbc.connect(
            'DRIVER=' + driver +
            ';SERVER=' + server +
            ';PORT=1433;DATABASE=' + database +
            ';UID=' + username +
            ';PWD=' + password
        )
        return conn
    except Exception:
        print('Connection Error')