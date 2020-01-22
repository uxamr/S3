import boto3
import pandas as pd
import os
import time

class Upload_S3() :
    def __init__(self, name) :
        session = boto3.Session(
            aws_access_key_id = os.environ["AWS_KEY"],
            aws_secret_access_key = os.environ["AWS_SECRET_KEY"])
    
        s3 = session.client('s3')
        
        t0 = time.time()
        for i in name :
            with open(i, "rb") as f:
                s3.upload_fileobj(f, os.environ["S3_Bucket"], i)
                
        t1 = time.time()
        
        print("Tiempo de subida a S3: ", t1-t0)
        
        
class Copy_Redshift() :
    def __init__(self, name_csv, name_table, engine) :
        csv = pd.read_csv(name_csv)
        cols = csv.columns
        with engine.connect() as conn :
            conn.execute("COPY {schema}.{table} ({cols}) FROM '{s3}' WITH CREDENTIALS 'aws_access_key_id={keyid};aws_secret_access_key={secretid}' CSV IGNOREHEADER 1 EMPTYASNULL;commit;".format(schema = os.environ["REDSHIFT_SCHEMA"], 
                                                                                                                                                                                                    table = name_table,
                                                                                                                                                                                                    cols = ', '.join(cols[j] for j in range( len(cols) ) ),
                                                                                                                                                                                                    s3='s3://{}/{}'.format(os.environ["S3_Bucket"],name_csv),
                                                                                                                                                                                                    keyid= os.environ["AWS_KEY"],
                                                                                                                                                                                                    secretid= os.environ["AWS_SECRET_KEY"]))
                                                        
    
    
    
    
