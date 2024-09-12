import azure.functions as func
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import text, create_engine
from sqlalchemy.types import NVARCHAR
from urllib.parse import quote_plus
from io import BytesIO
import smtplib
import email.message
import os
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

def enviar_email(subject: str, corpo_email: str):
    try:
        msg = email.message.Message()
        msg['Subject'] = subject
        msg['From'] = 'your-email@example.com'
        msg['To'] = 'recipient-email@example.com'
        password = os.environ.get('EMAIL_PASSWORD')  # Senha do email usando variáveis de ambiente
        msg.add_header('Content-Type', 'text/html')
        msg.set_payload(corpo_email)

        s = smtplib.SMTP('smtp.office365.com', 587)
        s.starttls()
        s.login(msg['From'], password)
        s.sendmail(msg['From'], [msg['To']], msg.as_string().encode('utf-8'))
        s.quit()
        logging.info('Email enviado')
    except Exception as e:
        logging.error(f"Erro ao enviar e-mail: {e}")

@app.blob_trigger(arg_name="myblob", path="{CONTAINERNAME}/{FILENAME}", 
                  connection=os.environ.get('AZURE_STORAGE_CONNECTION_STRING')) #Connection string do blob usando variáveis de ambiente
def blob_trigger(myblob: func.InputStream):
    logging.info("Function triggered.")
    logging.info(f"Python blob trigger function processed blob\n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    try:
        df = pd.read_excel(BytesIO(myblob.read()))
        logging.info("DataFrame carregado com sucesso")
        logging.info(f"DataFrame preview:\n{df.head()}")

        parametros = (
        'DRIVER={ODBC Driver 18 for SQL Server};'
        f"SERVER={os.environ.get('DB_SERVER')};" # Nome do servidor usando variáveis de ambiente
        'PORT=1433;'
        f"DATABASE={os.environ.get('DB_NAME')};" # Nome do banco de dados usando variáveis de ambiente
        f"UID={os.environ.get('DB_USER')};"     # Usuário do banco de dados usando variáveis de ambiente
        f"PWD={os.environ.get('DB_PASSWORD')}" # Senha do banco de dados usando variáveis de ambiente
        )

        types = dict()
        for col in df.columns:
            types[col] = NVARCHAR(length=250)
        
        url_db = quote_plus(parametros)
        engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % url_db, fast_executemany=True, use_setinputsizes=False)
        
        df.to_sql('your_table_name', engine, if_exists='append', index=False, dtype=types)
        
        logging.info("Inserido com sucesso no banco")

        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=os.environ.get('AZURE_STORAGE_CONNECTION_STRING') #Connection string do blob usando variáveis de ambiente
        )
        container_client = blob_service_client.get_container_client("{CONTAINERNAME}")
        blob_client = container_client.get_blob_client("{FILENAME}")
        blob_client.delete_blob()
        logging.info("Arquivo deletado do container.")
        enviar_email(
            subject="Sucesso ao inserir no banco",
            corpo_email="<p>O arquivo foi inserido no banco com sucesso!!!!</p>"
        )

    except Exception as e:
        logging.error(f"Erro ao inserir o arquivo no banco: {e}")
        enviar_email(
            subject="Falha ao inserir arquivo no banco de dados",
            corpo_email=f"<p>Ocorreu um erro ao tentar inserir o arquivo no banco: {e}</p>"
        )
