#!env/bin/python
# -*- coding: utf-8 -*-
import json, csv
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf, sql

class Travel:
    def __init__(
        self, Identificador_do_processo_de_viagem,
        Situacao,
        Codigo_orgao_superior,
        Nome_orgao_superior,	
        Codigo_orgao_solicitante,	
        Nome_orgao_solicitante,	
        CPF_viajante,	
        Nome,	
        Cargo,
        Periodo_Data_início,
        Periodo_Data_de_fim,	
        Destinos,	
        Motivo,	
        Valor_diarias,
        Valor_passagens,	
        Valor_outros_gastos
    ):
        self.Identificador_do_processo_de_viagem =  Identificador_do_processo_de_viagem,
        self.Situacao = Situacao,	
        self.Codigo_orgao_superior = Codigo_orgao_superior,	
        self.Nome_orgao_superior = Nome_orgao_superior,	
        self.Codigo_orgao_solicitante = Codigo_orgao_solicitante,	
        self.Nome_orgao_solicitante = Nome_orgao_solicitante,	
        self.CPF_viajante = CPF_viajante,	
        self.Nome = Nome,	
        self.Cargo = Cargo,	
        self.Periodo_Data_início = Periodo_Data_início,	
        self.Periodo_Data_de_fim = Periodo_Data_de_fim,	
        self.Destinos = Destinos,	
        self.Motivo = Motivo,	
        self.Valor_diarias = Valor_diarias,	
        self.Valor_passagens = Valor_passagens,	
        self.Valor_outros_gastos = Valor_outros_gastos


    def order_file():
        path_nome = '/mnt/d/projPython/projTravel/2020_Viagem.csv'
        with open (path_nome, "r") as read_file:
            next (read_file)
            for line in read_file:
                dados = line
                s1 = dados.split(';')
                #order by  
                try:
                    #SUM "Valor Total" (Valor diária + Valor Passagens + Valor Outros) 
                    valor_total = float(s1[14].replace('"', "").replace(',', '.')) + float(s1[15].replace('"', "").replace(',', '.'))
                    print(
                    s1[14].replace('"', ""),
                    s1[15].replace('"', ""),
                    valor_total
                    )
                except Exception as identifier:
                    print('error here ";" inside the text', identifier) 
    
    def 
                
    def generate_file():
        spark = SparkSession \
            .builder \
            .appName("Travel csv to Parquet") \
            .config("spark.master", "local") \
            .getOrCreate()

        #DataFrameReader 
        df_viagem = spark.read.csv("/mnt/d/projPython/projTravel/2020_Viagem.csv")
        #df_viagem.show()

        #generate a Parquet file
        df_viagem.write.mode("overwrite").options(header="true",sep=";").parquet("/mnt/d/projPython/projTravel/2020_Viagem.parquet")
        #df_viagem.write.mode("overwrite").options(header="true",sep=";").parquet("/mnt/d/projPython/projTravel/2020_Viagem.parquet")

        #generate a json file
        df_viagem.write.mode("overwrite").json("/mnt/d/projPython/projTravel/2020_Viagem.json")

        #generate a xml file
        #df_viagem.write.mode("overwrite").xml("/mnt/d/projPython/projTravel/2020_Viagem.xml")

        
if __name__ == "__main__":
    travel = Travel 
    travel.order_file()
    travel.generate_file()