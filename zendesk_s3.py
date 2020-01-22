import os 
from env import env
from utils import fechas, Redshift, History, Webpath, New_Columns, Initialize
from aws_utils import Upload_S3, Copy_Redshift
import requests
from itertools import zip_longest
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import pandas as pd 
import time

class Zendesk(env, Redshift) : 
    def __init__(self, start, end): 
        super().__init__()
        self.session = requests.Session()
        self.session.headers = {
            "Authorization": 
                            "Bearer {}".format(os.environ["ZENDESK_TOKEN"])
        }
        self.search_query = {
            "q": "timestamp:[{0} TO {1}]".format(start, end)
        }
        self.__extraccion()

    def __extraccion(self):
        response = self.session.get(os.environ["ZENDESK_URL_SEARCH"], params = self.search_query)
        print("Total de tickets a extraer: "+ " " + str(response.json()['count']))
        ids = []
        datos = response.json()['results']
        ids_ =[chat["id"] for chat in datos]
        ids.extend(ids_)
        while response.json()["next_url"]: 
            url = response.json()["next_url"]
            response = self.session.get(url)
            chats = response.json()['results']
            ids_ = [chat["id"] for chat in chats]
            ids.extend(ids_)
        self.ids = ids

        def grouper(n, iterable, fillvalue = None):
            args = [iter(iterable)]*n
            return zip_longest(fillvalue= None, *args)
        
        def bulk(lista): 
            chats = []
            for group in grouper(50,lista): 
                filtro = [id for id in group if id is not None]
                ids = ",".join(filtro)
                params = {"ids":ids}
                url = os.environ["ZENDESK_URL_CHATS"]
                response = self.session.get(url, params = params)
                stats = response.json()['docs']
                for i in stats: 
                    chats.append(stats[i])
            return chats
        self.chats = bulk(self.ids)
        self.__tabla_completa(self.chats)
        

    def __tabla_completa(self, datos):
        # funciones internas
        def clean_lists(datos):
            salida = []
            for i in datos:
                if type(i) == float: 
                    salida.append("Sin nombre")
                elif len(i) <1: 
                    salida.append("Sin nombre")
                else: 
                    salida.append(i[0])
            return salida

        def clean_tags(datos): 
            salida = []
            for i in datos:
                if type(i) == float: 
                    salida.append("Sin tags")
                else: 
                    salida.append("_".join(i))
            return salida
        def fix_columns(datos) :
            columns = datos.columns
            for i in columns :
                datos = datos.rename( columns = {i:"{}".format(i.replace(".", "_"))} )
                #En algun momento aparece una columna llamada 'to' que produce syntax error
                try : 
                    datos = datos.rename(columns = {'to': 'to_'})
                except :
                    pass
            return datos
        
        tabla = pd.DataFrame()      
        for i in datos: 
            tabla = tabla.append(json_normalize(i), ignore_index = True, sort=True)
        tabla = tabla.drop(columns =['session.user_agent'])
        try: 
            tabla = tabla.drop(columns = ['message'])
        except:
            pass
        
        print("Tickets Extraidos : " + str(len(tabla)))
        
        tabla["agent_names"] = clean_lists(tabla['agent_names'])
        tabla['agent_ids'] = clean_lists(tabla['agent_ids'])
        tabla['tags'] = clean_tags(tabla['tags'])
        
        
        
        #Construye un dataframe para history y otro para webpath
        tabla_h = History(tabla).history
        tabla_wp = Webpath(tabla).webpath
        
        try:
            tabla_h['tags'] = clean_tags(tabla_h['tags'])
        except :
            pass
        
        
        try :
            tabla_h['new_tags'] = clean_tags(tabla_h['new_tags'])
        except :
            pass
        
        tabla = tabla.drop(columns=['history','webpath'])
        
        tabla['triggered_response'].fillna('False', inplace = True)
        tabla = tabla.astype({'triggered_response' : 'bool'})
        
        tabla['missed'].fillna('False', inplace = True)
        tabla = tabla.astype({'missed' : 'bool'})
        
        tabla['triggered'].fillna('False', inplace = True)
        tabla = tabla.astype({'triggered' : 'bool'})
        
        tabla['unread'].fillna('False', inplace = True)
        tabla = tabla.astype({'unread' : 'bool'})
            
#        print("Tickets extraidos (history)" ,len(tabla_h['id'].value_counts()))
#        print("Tickets extraidos (webpath)" ,len(tabla_wp['id'].value_counts()))
        
        tabla = fix_columns(tabla)
        tabla_h = fix_columns(tabla_h)
        tabla_wp = fix_columns(tabla_wp)
        
        tabla = tabla.astype({'response_time_avg' : 'float64'})
        tabla = tabla.astype({'response_time_first' : 'float64'})
        tabla = tabla.astype({'response_time_max' : 'float64'})
        
        self.history = tabla_h    
        self.webpath = tabla_wp
        self.tabla = tabla
        return tabla
 
if __name__ == "__main__"    : 
    env()    
    #Lista de parejas de fechas que representan un mes, empezando desde la fecha de entrada hasta la actualidad 
    lista =  fechas("2018-04-01").fechas
    
    engine = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:{port}/{base}".format( user = os.environ['REDSHIFT_USER'], 
                                                                                            contr= os.environ["REDSHIFT_PASSWORD"],
                                                                                            port= os.environ["REDSHIFT_PORT"],
                                                                                            base= os.environ["REDSHIFT_DB"], 
                                                                                            host= os.environ["REDSHIFT_HOST"] ), 
                               connect_args={'options': '-csearch_path={schema}'.format( schema = os.environ["REDSHIFT_SCHEMA"] )}, echo = False)

    
    j = 0
    t = []
    h = []
    w = []
    
    for i in lista :
        print("Periodo: ", i)
        tablas = Zendesk("{}".format(i[0]), "{}".format(i[1]))
        tabla = tablas.tabla
        history = tablas.history
        webpath = tablas.webpath
       
                
        #En la primera iteracion borra la tabla de redshift (si existe) y crea una nueva
        if j == 0 :
            Initialize('tickets_prueba', engine, key = 'p')
            Initialize('history_prueba', engine, key = 'f', ref = 'tickets_prueba')
            Initialize('webpath_prueba', engine, key = 'f', ref = 'tickets_prueba')
    
        
        #Inserta las columnas nuevas en las bases de datos
        New_Columns(tabla, engine,'tickets_prueba')
        New_Columns(history, engine,'history_prueba')
        New_Columns(webpath, engine,'webpath_prueba')
        
        t.append(tabla)
        h.append(history)
        w.append(webpath)
        
        
        j = j+1
        
        
        
    df_tickets = pd.concat(t, sort = False)
    df_history = pd.concat(h, sort = False)
    df_webpath = pd.concat(w, sort = False)
    
    print("Tickets totales: ", len(df_tickets))   
            
    #Se guardan los CSV
    df_tickets.to_csv('tickets.csv', index = False)
    df_history.to_csv('history.csv', index = False)
    df_webpath.to_csv('webpath.csv', index = False)
    
    #Se cargan los CSV a S3
    Upload_S3(['tickets.csv', 'history.csv', 'webpath.csv'])
    
    #Se hace el COPY a redshift
    t0 = time.time()
    Copy_Redshift('tickets.csv', 'tickets_prueba', engine)
    Copy_Redshift('history.csv', 'history_prueba', engine)
    Copy_Redshift('webpath.csv', 'webpath_prueba', engine)
    t1 = time.time()
    print("Tiempo de COPY S3 -> Redshift: ", t1-t0)
    
        

