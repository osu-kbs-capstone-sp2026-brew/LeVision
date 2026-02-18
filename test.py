import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BALLDONTLIE_API_KEY = os.getenv("BALLDONTLIE_API_KEY")

API_URL = "https://api.balldontlie.io/v1/teams"

headers = {
    "Authorization": BALLDONTLIE_API_KEY
}

def get_all_teams():
    response = requests.get(API_URL, headers=headers)

    if response.status_code != 200:
        print("Error:", response.status_code)
        print(response.text)
        return []

    data = response.json()
    return data["data"]

def display_team_names():
    teams = get_all_teams()

    print("\n=== NBA Teams ===\n")

    for team in teams:
        print(team["full_name"])

if __name__ == "__main__":
    display_team_names()
