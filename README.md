Casual Football Finance Administrator.

This is a db backend package for CFFA web service. This requires a MongoDB backend.

This PoC project is to primarily learn basic python programming with Auth0 and MongoDB alongside Flask. 

In term this provides a platform to deploy the service in docker containers and subsequently
learn kubernetes deployment into the public cloud.

Further to documentation might follow.

Note the following .env should be created by the end user:

AUTH0_CLIENT_ID=<Your Auth0 Client ID>

AUTH0_DOMAIN=<Your Auth0 Domain>

AUTH0_CLIENT_SECRET=<Your Auth0 Client Secret>

AUTH0_CALLBACK_URL=https:<Your website>/callback

AUTH0_AUDIENCE=https://<Your Auth0 Domain>/userinfo

BACKEND_DBPWD=<MongoOB password>

BACKEND_DBUSR=<MonghoDB username>

BACKEND_DBHOST=<MongoDB Hostname/s>

BACKEND_DBPORT=<MongoDB Port>

BACKEND_DBNAME=<MongoDB Database Name, eg footballDB>

SECRET_KEY=notForProduction

PYTHONPATH=<Path to cffa:and cffadb>

EXPORTDIRECTORY=<Local dir to export db archives>

CFFA_USERID=Auth0 Username including auht0| prefix>

To import data from a google sheet the following needs to be set. NB: documentation on googlesheet template to follow

GOOGLEKEYFILE=<json file to access the google sheet>

GOOGLE_SHEET="<sheet name>

TRANSACTION_SRC_WKSHEET=Sheet name in google sheet with money transfer data

GAME_SRC_WKSHEET=Sheet in google sheet with game data

SUMMARY_SRC_WKSHEET=Sheet in google sheet with summary data
