#!/usr/bin/python3
import os
import requests
from tqdm import tqdm
import configparser
import argparse
import sqlite3
import re

# Help et arguments
parser = argparse.ArgumentParser(description='Import contact to newsletter Infomaniak from Notion database.')
parser.add_argument('--all', action='store_true', help='Check all databases, not just 100 last.')
parser.add_argument('--pull', action='store_true', help='Retreive contact list from Infomaniak.')
args = vars(parser.parse_args())


# Récupération des informations dans le fichier de config
config = configparser.ConfigParser()
config.read('config.ini')
pages = config.getint('DEFAULT', 'PAGES')
database_id = config.get('DEFAULT', 'DATABASE_ID')
notion_token = config.get('DEFAULT', 'NOTION_TOKEN')
infomaniak_access_token = config.get('DEFAULT', 'INFOMANIAK_ACCESS_TOKEN')
infomaniak_secret_token = config.get('DEFAULT', 'INFOMANIAK_SECRET_TOKEN')
mailing_list_id = config.get('DEFAULT', 'MAILING_LIST_ID')

# Création des variables pour connexion à Notion
headers = {
    "Authorization": "Bearer " + notion_token,
    "Content-Type": "application/json",
    "Notion-Version": "2021-05-13"
}



# Configuration de la base de données
conn = sqlite3.connect('contacts.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='contacts'")
table_exists = cursor.fetchone()
if not table_exists:
    cursor.execute('''CREATE TABLE contacts
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nom TEXT,
                    prenom TEXT,
                    email TEXT)''')

# Récupération de la base de données Notion avec pagination
def readDatabase(databaseId, headers, pages):
    readUrl = f"https://api.notion.com/v1/databases/{databaseId}/query"
    if args['all']:
        nb_pages=0
        print("\nRécupération des données Notion en cours ... ")
        pbar = tqdm(total=pages)

        results = []
        next_cursor = None

        while True:
            pbar.update(1)
            payload = {}
            if next_cursor:
                payload["start_cursor"] = next_cursor

            res = requests.post(readUrl, headers=headers, json=payload)
            
            if res.status_code != 200:
                print(f"Error: {res.status_code}")
                break
            #print("Acquisitions des données en cours ...")
            nb_pages+=1
            json_data = res.json()
            results += json_data["results"]
            next_cursor = json_data.get("next_cursor")

            if not next_cursor:
                break
            
        pbar.close()
        config.set('DEFAULT', 'PAGES', str(nb_pages))
        with open('config.cfg', 'w') as configfile:
            config.write(configfile)
    else:
        res = requests.post(readUrl, headers=headers)
            
        if res.status_code != 200:
            print(f"Error: {res.status_code}")
            exit(1)

        json_data = res.json()
        results = json_data["results"]
    return results


# Création du contact dans la mailing list Infomaniak
def contact2infomaniak(contact,mailing_list_id,infomaniak_access_token,infomaniak_secret_token):
    url = f"https://newsletter.infomaniak.com/api/v1/public/mailinglist/{mailing_list_id}/importcontact"
    headers = {
        "Content-Type": "application/json"
    }
    auth = (infomaniak_access_token, infomaniak_secret_token)
    data = {
        "contacts": [
            {
                "email": contact['email'],
                "Nom": contact['nom'],
                "firstname": contact['prenom']
            }
        ]
    }
    try:
        response = requests.post(url, headers=headers, auth=auth, json=data)
        return response.json()['result']
    except:
        return "error"

def getContactListFromIK(mailing_list_id,infomaniak_access_token,infomaniak_secret_token):
    url = f"https://newsletter.infomaniak.com/api/v1/public/mailinglist/{mailing_list_id}/contact?perPage=10000"
    headers = {
        "Content-Type": "application/json"
    }
    auth = (infomaniak_access_token, infomaniak_secret_token)
    contacts = []
    try:
        response = requests.get(url, headers=headers, auth=auth)
        for contact in response.json()['data']['data']:
            nom = "ik"
            prenom = "ik"
            email = contact["email"]
            if re.match(r"[^@]+@[^@]+\.[^@]+", email):
                contacts.append({"nom": nom, "prenom": prenom, "email": email})
        return contacts
    except:
        print("error")


def formatNotion(json_data):
    success = 0
    failed = 0
    contacts = []
    for contact in json_data:
        try:
            nom = contact["properties"]["Nom"]["title"][0]["text"]["content"]
            prenom = contact["properties"]["Prénom"]["rich_text"][0]["text"]["content"]
            email = contact["properties"]["mail"]["email"]
            if re.match(r"[^@]+@[^@]+\.[^@]+", email):
                contacts.append({"nom": nom, "prenom": prenom, "email": email})
                success+=1
            else:
                failed+=1

        except:
            failed+=1
    return contacts, success, failed


def checkExist(contact):
    try:
        cursor.execute("SELECT email FROM contacts WHERE email=?", (contact['email'],))
        result = cursor.fetchone()
        if result is None:
            return False
        else:
            return True
    except:
        pass


def send2localdb(contact):
    try:
        conn.execute("INSERT INTO contacts (nom, prenom, email) VALUES (?, ?, ?)",
                    (contact['nom'], contact['prenom'], contact['email']))
        print(f"{contact['email']} ajoutée à la base de données !")
    except:
        pass

# Let's go
if args['pull']:
    contacts = getContactListFromIK(mailing_list_id,infomaniak_access_token,infomaniak_secret_token)
    print(f"\nTraitement des {len(contacts)} contacts en cours ... ")
    pbar = tqdm(total=len(contacts))
    for contact in contacts:
        pbar.update(1)
        if not checkExist(contact):
                send2localdb(contact)
    pbar.close()
    conn.commit()
    conn.close()
else:
    json_data = readDatabase(database_id, headers, pages)
    contacts, success, failed = formatNotion(json_data)
    print(f"\nTraitement des {len(contacts)} contacts en cours ... ")
    pbar = tqdm(total=len(contacts))
    for contact in contacts:
        pbar.update(1)
        if not checkExist(contact):
            if contact2infomaniak(contact, mailing_list_id, infomaniak_access_token ,infomaniak_secret_token) == "success":
                send2localdb(contact)
    pbar.close()
    print(f"\nMails trouvés : {success}")
    print(f"Contacts mal renseignés: {failed}")
