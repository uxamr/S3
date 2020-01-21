from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from sqlalchemy import create_engine
from pandas.io.json import json_normalize
import pandas as pd
import os

class fechas(): 
    def __init__(self, inicio = "2017-11-01"):
        """
        inicio: fecha en formato %Y-%m-%d. Este valor se tomara como referencia para el calculo del intervalo. DEFAULT = 2017-11-01
        Solo para extracciones completas
        """
        delta = relativedelta(months =+ 1)
        date1 = datetime.strptime(str(inicio), "%Y-%m-%d")
        d = date1
        salida = []
        while d<=datetime.strptime(str(date.today()), "%Y-%m-%d"): 
            salida.append(d.strftime("%Y-%m-%d"))
            d += delta
        self.fechas = salida
        self.ends = self.__last_day(self.fechas)
        self.fechas = [(start, end) for start, end in zip(self.fechas, self.ends)]
    
    def __last_day(self, lista):
        end_list = []
        for i in lista: 
            dia = monthrange(int(i[:4]),int(i[5:7]))
            end_list.append("{}-{}-{}".format(i[:4],i[5:7],dia[1]))
        return end_list

class Redshift(): 
	def __init__(self): 
		"""Motor para la conexion con Redshift"""
		self.motor = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:5439/{base}".format(user = os.environ['REDSHIFT_USER'], 
																										contr = os.environ['REDSHIFT_PASSWORD'], 
																										base = "runastaging",
																										host = os.environ['REDSHIFT_HOST']),
																										connect_args={'sslmode': 'prefer',
																													  'options': '-csearch_path={}'.format("zendesk_chats")},
																									echo = False, encoding = 'utf8')

class Semana(): 
    def __init__(self, dias = 7): 
        """
        Para extraccion semanal en cronjob. El valor por default de dias que se tomara es de 7  
        """
        today = date.today()
        dias = timedelta(days= dias)
        self.fechas = (str(today-dias), str(date.today()))
        
#Crea un dataframe con las entradas de history
class History() :
    def __init__(self, datos) :
        hist = datos['history']
        ids_h = datos['id']
        aux_df = pd.DataFrame()
        for i in range(len(hist)) :
            try :
                temp = json_normalize(hist[i])
                temp['id'] = ids_h[i]
                aux_df = aux_df.append(temp, sort=False)
            except :
                pass
        try:
            aux_df = aux_df.drop(columns=['index'])
        except:
            pass
#        aux_df = aux_df.drop(columns=['msg'])
        
        self.history = pd.merge(datos['id'], aux_df, how='outer', on='id')
        

#Crea un dataframe con las entradas de webpath
class Webpath() :
    def __init__(self, dat) :
        def Reduce(column) :
            temp = []
            for i in column :
#                print(i)
                if i is not None and len(i) > 5 and len( i[i.rindex('/')+1 : ] ) > 50 :
                    temp.append( i[ : i.rindex('/')+1] )
                else :
                    temp.append(i)
            return temp
        wp = dat['webpath']
        ids = dat['id']
        aux_df = pd.DataFrame()
        for i in range(len(wp)) :
            try :
                temp = json_normalize(wp[i])
                temp['id'] = ids[i]
                aux_df = aux_df.append(temp, sort=False)
            except :
                pass
        aux_df['to_'] = Reduce(aux_df['to'])
        aux_df = aux_df.drop(columns = ['to'])
        aux_df['from_'] = Reduce(aux_df['from'])
        aux_df = aux_df.drop(columns = ['from'])
        self.webpath = pd.merge(dat['id'], aux_df, how='outer', on='id')
        
#Regresa las columnas nuevas del dataframe que se quiere insertar que no estan en la bd y las inserta
class New_Columns() :
    def __init__(self, tabla, eng, name) :
        types = {"int": "bigint",
                 "int64": "bigint",
                 "bool": "boolean",
                 "float64": "double precision",
                 "object": "character varying (65535)"}    
        cols = tabla.columns
        with eng.connect() as conn :
            try :
                q = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{}'".format(name))
                cols_db = []
                for i in q :
                    cols_db.append(i[0])
    #            print("Columnas en la db ", cols_db)
                if len(cols_db) == 0:
                    self.cols = cols_db
                else :
                    self.cols = list(set(cols) - set(cols_db))
            except :
                self.cols = []
            #Agrega las columnas nuevas que se tengan que agregar
            if len(self.cols) != 0 :
                print("Columnas nuevas: ", self.cols)
                for j in self.cols :
                    #Muestra el tipo de dato de las columnas nuevas
#                    print(j, str(tabla[j].dtypes))
                    conn.execute("ALTER TABLE {} ADD COLUMN {} {}".format(name, str(j),types[ str(tabla[j].dtypes) ]))
                    
class Initialize() :
    def __init__(self, name, engine, key = 'f', ref = 'tickets_prueba') :
        with engine.connect() as conn :
            conn.execute("DROP TABLE IF EXISTS {} CASCADE".format(name))
            if key == 'p' :
                conn.execute("CREATE TABLE {}(id character varying (1024) PRIMARY KEY)".format(name))
            else :
                conn.execute("CREATE TABLE {}(id character varying (1024), FOREIGN KEY (id) REFERENCES {} (id))".format(name,ref))
    
    
    
    
    