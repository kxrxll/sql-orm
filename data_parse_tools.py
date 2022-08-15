import json


def parse_json():
    with open('data.json') as f:
        data = json.load(f)
        return data
