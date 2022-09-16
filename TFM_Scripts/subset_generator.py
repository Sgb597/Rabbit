#!/bin/bash

import pandas as pd
pd.options.display.max_columns = None

event_file = '/home/sebastian/Documents/TFM/data/eventos_filtrados.csv'
event_df = pd.read_csv(event_file, sep=";")
event_id_set = set(event_df['Id_Vehiculo'].unique().tolist())

cantrama_file = '/home/sebastian/Documents/TFM/data/cantrama_filtrados.csv'
cantrama_df = pd.read_csv(cantrama_file, sep=";")
cantrama_id_set = set(cantrama_df['Id_Vehiculo'].unique().tolist())

matching_ids = list(event_id_set.intersection(cantrama_id_set))
event_dict = event_df[event_df['Id_Vehiculo'].isin(matching_ids)]['Id_Vehiculo'].value_counts().to_dict()
cantrama_dict = cantrama_df[cantrama_df['Id_Vehiculo'].isin(matching_ids)]['Id_Vehiculo'].value_counts().to_dict()

ideal_Ids = [key for key in event_dict if event_dict[key] > 50 and cantrama_dict[key] > 10]

#EVENTO HISTORICO CSVs
for i in ideal_Ids:
    _ = event_df[event_df['Id_Vehiculo'] == i]
    path = '/home/sebastian/Desktop/TestingCSVs/event_df'+str(i)+'.csv'
    _.to_csv(path, sep=';', index=False)

#CANTRAMA CSVs
for i in ideal_Ids:
    _ = cantrama_df[cantrama_df['Id_Vehiculo'] == i]
    path = '/home/sebastian/Desktop/TestingCSVs/cantrama_df'+str(i)+'.csv'
    _.to_csv(path, sep=';', index=False)
