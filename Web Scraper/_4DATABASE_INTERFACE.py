from sshtunnel import SSHTunnelForwarder
from getpass import getpass
import paramiko
import os
from pathlib import Path
import io
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
''' Handles all connection related tasks to the postgresql server. '''


''' assumes pkey is named privatekey and is in same dir as script. '''
SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
PRIVATEKEYPATH = os.path.join(SCRIPTDIR, 'privatekey')
PRIVATEKEYPATH = Path(PRIVATEKEYPATH)

''' config for connecting to pi '''
RASPSERVER = ('vincevpn.ddns.net', 888, 'guest')

def CheckKey():
    ''' check for a valid key '''
    if os.path.isfile(PRIVATEKEYPATH) is False:
        print("No private key for ssh connection to server detected.")
        print(f"Place private key in {SCRIPTDIR}")
        print("~~~~ FAILING ~~~~")
        exit(1)

def InputPassword():
    ''' ask for password (hidden input)'''
    CheckKey()
    password = getpass('Enter password... ')
    return password

def InputString():
    ''' ask for string '''
    CheckKey()
    string = input()
    return string

def ReadKey(pKeyPath):
    ''' read private key file '''
    CheckKey()
    f = open(pKeyPath, 'r')
    s = f.read()
    f.close()
    keyFile = io.StringIO(s)
    return keyFile

def DecodeKey(pKey):
    ''' decode private key with given password '''
    CheckKey()
    print(f"Enter password for private key: ")
    password = InputPassword()
    try:
        privateKey = paramiko.Ed25519Key.from_private_key(pKey, password=password)
    except Exception as e:
        print(e)
        exit(1)
    return privateKey

def InitializeTunnel(address, port, username, pkey):
    ''' setup tunnel configuration '''
    tunnel =  SSHTunnelForwarder(
        (address, port),
        ssh_username=username,
        ssh_pkey=pkey,
        remote_bind_address=('192.168.168.15', 5432) #postgresql's listen port, pi's addr.
    )
    return tunnel

def SSHTunnel():
    ''' setup tunnel config. and find all necessary dependencies '''
    pKey = ReadKey(PRIVATEKEYPATH)
    decryptedPKey = DecodeKey(pKey)
    tunnel = InitializeTunnel(*RASPSERVER, decryptedPKey)
    return tunnel

def GetDBCredentials():
    ''' ask for credentials to database '''
    CheckKey()
    print("Refer to github readme.")
    print("Enter database username: ")
    username = InputString()
    print("Enter database password: ")
    password = InputPassword()

    return [username, password]

def DisconnectEngine(engine):
    print("Cleaning up database engine connection...")
    engine.dispose()

def OutputDFToSQL(df, region, engine, schema, localPort):
    ''' output dataframe to sql to postgresql server '''
    df.to_sql(region, engine, schema=schema, if_exists='replace', chunksize = 100, index=False)

def connectDatabase(dbCredentials, localPort):
    engine = create_engine(f'postgresql://{dbCredentials[0]}:{dbCredentials[1]}@localhost:{localPort}/zealty', paramstyle='format')
    return engine

def SQLTableToDF(tableName, engine, schema):
    df = pd.read_sql_table(tableName, engine, schema=schema)
    return df