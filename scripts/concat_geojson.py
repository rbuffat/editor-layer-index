#!/usr/bin/env python
import json
import sys
import io
from datetime import datetime

source_features = []
for file in sys.argv[1:]:
    with io.open(file, 'r') as f:
        # simplify all floats to 5 decimal points
        source_features.append(json.load(f, parse_float=lambda x: round(float(x), 5)))

generated = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.utcnow())
version = "1.0"

collection = {
    "type": "FeatureCollection",
    "meta": {
        "generated": generated,
        "format_version": version
    },
    "features": source_features
}

with open('imagery.geojson', 'w', encoding='utf-8') as out:
    json.dump(collection, out, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    out.write("\n")
