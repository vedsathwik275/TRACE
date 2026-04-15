import os
import json
from dotenv import load_dotenv
from firecrawl import Firecrawl

load_dotenv()

api_key = os.getenv("FIRECRAWL_API_KEY")
assert api_key, "FIRECRAWL_API_KEY not set in .env"

app = Firecrawl(api_key=api_key)

url = "https://www.basketball-reference.com/players/d/duranke01.html"

prompt = (
    "From the Advanced Stats table on this Basketball Reference player page, "
    "extract each season row. For each season return: season (e.g. 2018-19), "
    "age, team abbreviation, games played (G), and Player Efficiency Rating (PER). "
    "Only include regular season rows, not career totals or did-not-play rows."
)

schema = {
    "type": "object",
    "properties": {
        "seasons": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "season": {"type": "string"},
                    "age": {"type": "integer"},
                    "team": {"type": "string"},
                    "games_played": {"type": "integer"},
                    "per": {"type": "number"},
                },
                "required": ["season", "games_played", "per"],
            },
        }
    },
}

result = app.extract([url], prompt=prompt, schema=schema)

print(json.dumps(result.data, indent=2))
