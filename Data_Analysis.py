import pandas as pd
import csv
from collections import defaultdict

def analysis():
    def return_list(disease):
        disease_list = []
        match = disease.replace('^','_').split('_')
        ctr = 1
        for group in match:
            if ctr % 2 == 0:
                disease_list.append(group)
            ctr += 1
        return disease_list

    disease_list = []
    dict_wt = {}
    dict_ = defaultdict(list)

    # Open the CSV file with error handling for encoding issues
    with open("Scraped-Data/dataset_uncleaned.csv", encoding='ISO-8859-1', errors='replace') as csvfile:
        reader = csv.reader(csvfile)
        disease = ""
        weight = 0
        for row in reader:
            if row[0] != "\xc2\xa0" and row[0] != "":
                disease = row[0]
                disease_list = return_list(disease)
                weight = row[1]

            if row[2] != "\xc2\xa0" and row[2] != "":
                symptom_list = return_list(row[2])

                for d in disease_list:
                    for s in symptom_list:
                        dict_[d].append(s)
                    dict_wt[d] = weight

    with open("Scraped-Data/dataset_clean.csv", "w", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for key, values in dict_.items():
            for v in values:
                key_encoded = key.encode('utf-8', errors='replace').decode('utf-8')
                writer.writerow([key_encoded, v, dict_wt[key]])

    columns = ['Source', 'Target', 'Weight']
    data = pd.read_csv("Scraped-Data/dataset_clean.csv", names=columns, encoding="utf-8")
    data.to_csv("Scraped-Data/dataset_clean.csv", index=False)

    slist = []
    dlist = []
    with open("Scraped-Data/nodetable.csv", "w", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        for key, values in dict_.items():
            for v in values:
                if v not in slist:
                    writer.writerow([v, v, "symptom"])
                    slist.append(v)
            if key not in dlist:
                writer.writerow([key, key, "disease"])
                dlist.append(key)

    nt_columns = ['Id', 'Label', 'Attribute']
    nt_data = pd.read_csv("Scraped-Data/nodetable.csv", names=nt_columns, encoding="utf-8")
    nt_data.to_csv("Scraped-Data/nodetable.csv", index=False)

    data = pd.read_csv("Scraped-Data/dataset_clean.csv", encoding="utf-8")
    print(len(data['Source'].unique()))
    print(len(data['Target'].unique()))

    return data

def analysis2(data):
    df = pd.DataFrame(data)
    df_1 = pd.get_dummies(df.Target)
    df_s = df['Source']
    df_pivoted = pd.concat([df_s, df_1], axis=1)
    df_pivoted.drop_duplicates(keep='first', inplace=True)
    cols = df_pivoted.columns[1:]
    df_pivoted = df_pivoted.groupby('Source').sum().reset_index()
    return df_pivoted
