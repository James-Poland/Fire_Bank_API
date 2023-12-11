from hashlib import sha256
import requests
from datetime import timedelta, datetime
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import time
import numpy as np
import json

url = "https://api.fire.com/business/v1/apps/accesstokens"

clientid = "<ClientIDHere>"
clientkey = "<KeyHere>"
nonce = int(time.time())
strnonce = str(nonce)
refreshtoken = "<TokenHere>"
_input = strnonce + clientkey
secret = sha256(_input.encode('utf-8')).hexdigest()
ican = 59307
limit = 200
now = int(datetime.now().timestamp())
yesterday = datetime.now() - timedelta(days=10)
yesterday_timestamp = str(int(yesterday.timestamp()))
nowdate = datetime.today().replace(microsecond=0)
nowstr = nowdate.strftime('%Y-%m-%d')
filename = r"C:\Temp\Transaction_"+nowstr+".csv"

#using sqlalchemy or something similar to convert df to sql insert is pretty easy from here, outputing csv for test

payload = {
    "clientId": clientid,
    "nonce": nonce,
    "grantType": "AccessToken",
    "refreshToken": refreshtoken,
    "clientSecret": secret
}
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

# print(response.text)

data = response.json()
df = pd.DataFrame.from_dict(data, orient='index').T.reset_index(drop=True)
token = df.at[0, 'accessToken']
expiry = df.at[0,'expiry']

time.sleep(30)

print("Token Retrieved:" + token)

app = "https://api.fire.com/business/v3/accounts/59307/transactions?limit=150&dateRangeFrom=" + yesterday_timestamp

headers = {
    "accept": "application/json",
    "authorization": "Bearer " + token
}

response = requests.get(app, headers=headers)

trans = response.json()

df = pd.json_normalize(trans)

json_response = json.dumps(trans)


##def transactions():
json_tree = json.loads(json_response)
if "content" in json_tree:
            transactions = json_tree['content']
            df = pd.json_normalize(transactions)
            x = df.fillna(value=np.nan)
            x['AmountBeforeCharges'] = np.where(x['type'] != 'LODGEMENT', x['amountBeforeCharges'] * -1,
            x['amountBeforeCharges']) / 100.00
            x['AmountAfterCharges'] = np.where(x['type'] != 'LODGEMENT', x['amountAfterCharges'] * -1,
            x['amountAfterCharges']) / 100.00
            x.rename(columns={'currency.description': 'currency', 'relatedParty.account.alias': 'SourceAccountName',
                      'relatedParty.account.nsc': 'nsc', 'relatedParty.account.accountNumber': 'accountNumber',
                      'relatedParty.account.bic': 'bic', 'relatedParty.account.iban': 'iban'}, inplace=True)

            file = x[["txnId", "ican", "feeAmount", "taxAmount", "date", "refId", "balance", "type", "myRef", 'SourceAccountName',
            'nsc', 'accountNumber', 'bic', 'iban', 'AmountBeforeCharges', 'AmountAfterCharges']].sort_values(by='date')
            file.to_csv(filename)
else:
    print(json_tree)




#transactions()
