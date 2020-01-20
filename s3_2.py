import boto3
import pandas as pd
from sqlalchemy import create_engine
import time

session = boto3.Session(
    aws_access_key_id = '',
    aws_secret_access_key = ''
    )

s3 = session.client('s3')

#for bucket in s3.buckets.all() :
#    print(bucket.name)

t0 = time.time()

    
with open("tickets.csv", "rb") as f:
    s3.upload_fileobj(f, "zendeskpruebas2", "tickets.csv")
    
with open("history.csv", "rb") as f:
    s3.upload_fileobj(f, "zendeskpruebas2", "history.csv")
    
with open("webpath.csv", "rb") as f:
    s3.upload_fileobj(f, "zendeskpruebas2", "webpath.csv")
    

engine = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:5439/{base}".format(user = '', 
                                                                                            contr='', 
                                                                                            base='', 
                                                                                            host=''), 
                                connect_args={'options': '-csearch_path={}'.format('zendesk_tickets')}, echo = True)


tickets = pd.read_csv("tickets.csv")
col_tickets = tickets.columns
print(  "Columnas : {cols}".format( cols = ', '.join(col_tickets[i] for i in range( len(col_tickets) ) ) ) )

history = pd.read_csv("history.csv")
col_history = history.columns

webpath = pd.read_csv("webpath.csv")
col_webpath = webpath.columns


with engine.connect() as conn :
    conn.execute("COPY zendesk_tickets.tickets_prueba ({cols}) FROM '{s3}' WITH CREDENTIALS 'aws_access_key_id={keyid};aws_secret_access_key={secretid}' CSV IGNOREHEADER 1;commit;".format(cols = ', '.join(col_tickets[i] for i in range( len(col_tickets) ) ),
                                                                                                                                              s3='s3://zendeskpruebas2/tickets.csv',
                                                                                                                                              keyid='',
                                                                                                                                              secretid=''))
    
    conn.execute("COPY zendesk_tickets.history_prueba ({cols}) FROM '{s3}' WITH CREDENTIALS 'aws_access_key_id={keyid};aws_secret_access_key={secretid}' CSV IGNOREHEADER 1;commit;".format(cols = ', '.join(col_history[i] for i in range( len(col_history) ) ),
                                                                                                                                              s3='s3://zendeskpruebas2/history.csv',
                                                                                                                                              keyid='',
                                                                                                                                              secretid=''))
    
    conn.execute("COPY zendesk_tickets.webpath_prueba ({cols}) FROM '{s3}' WITH CREDENTIALS 'aws_access_key_id={keyid};aws_secret_access_key={secretid}' CSV IGNOREHEADER 1;commit;".format(cols = ', '.join(col_webpath[i] for i in range( len(col_webpath) ) ),
                                                                                                                                              s3='s3://zendeskpruebas2/webpath.csv',
                                                                                                                                              keyid='',
                                                                                                                                              secretid=''))
    
t1 = time.time()

print("Tiempo de subida a S3 y COPY a Redshift: ", t1-t0) 