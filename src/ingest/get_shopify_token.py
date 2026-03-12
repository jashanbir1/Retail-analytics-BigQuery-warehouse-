import os
from pathlib import Path

import requests
from dotenv import load_dotenv

"""
__file__ = current script path
.resolve() = full absolute path
.parents[0] = src/ingest
.parents[1] = src
.parents[2] = project root

basically saying "Go up from this script until you reach the main project folder"
"""
PROJECT_ROOT = Path(__file__).resolve().parents[2]

#This loads the .env file from my project root so Python can access
load_dotenv(PROJECT_ROOT / ".env")

#details of shopify app and API in .env
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
SHOPIFY_CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID")
SHOPIFY_CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")


def validate_env() -> None:
    missing = [
        #This checks whether any required environment variables are missing.
        name
        for name, value in {
            "SHOPIFY_STORE_DOMAIN": SHOPIFY_STORE_DOMAIN,
            "SHOPIFY_CLIENT_ID": SHOPIFY_CLIENT_ID,
            "SHOPIFY_CLIENT_SECRET": SHOPIFY_CLIENT_SECRET,
        }.items()
        if not value
        #If this value is empty or missing, add its name to the missing list.
        #So if .env forgot the client secret, then: missing = ["SHOPIFY_CLIENT_SECRET"]
    ]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    #If anything is missing, stop the script immediately and show a clear error.



def get_shopify_access_token() -> str:
    validate_env()

    token_url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/oauth/access_token"

    """
    repoonse requests.post(...)
    Sends a POST request, because you are submitting credentials and asking for a token.

    headers={"Content-Type": "application/x-www-form-urlencoded"}
    Tells Shopify how the data is being sent.

    data={...}
    This is the payload sent to Shopify:
        -grant_type="client_credentials"
    tells Shopify you want a server-to-server token
        -client_id
    identifies your app
        -client_secret
    proves your app is authorized
    """

    response = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": SHOPIFY_CLIENT_ID,
            "client_secret": SHOPIFY_CLIENT_SECRET,
        },
        timeout=30,
    )

    """
    If Shopify does not return HTTP 200 OK, this throws an error.

    For example:
        - 400 = bad request
        - 401 = unauthorized
        - 403 = forbidden
    
    just a check
    
    """
    if response.status_code != 200:
        raise RuntimeError(
            f"Token request failed.\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}"
        )
    #convert the reponse JSON to a python dictionairy (remmember, json is a dictionary)
    payload = response.json()


    if "access_token" not in payload:
        raise RuntimeError(f"No access_token in response: {payload}")

    return payload["access_token"]

def main():
    token = get_shopify_access_token()
    print("Token request succeeded")
    print(f"Access token starts with {token[:12]}...")

if __name__ == "__main__":
    main()

