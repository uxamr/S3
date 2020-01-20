#Subir un archivo a S3 usando Boto3

import boto3

session = boto3.Session(
    aws_access_key_id = 'MY_KEY',
    aws_secret_access_key = 'MY_SECRET_KEY'
    )

s3 = session.client('s3')

#for bucket in s3.buckets.all() :
#    print(bucket.name)
    
with open("archivo.algo", "rb") as f:
    s3.upload_fileobj(f, "bucket", "nombre_de_archivo")
