import argparse
import asyncio
import glob
import json
import logging
import os
import re
from collections import namedtuple
from urllib.parse import urlparse
import aiohttp
import imagehash
import mercantile
from shapely.geometry import shape, Point
import aiofiles
from aiohttp import ClientSession
from PIL import Image
from io import BytesIO

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='Update WMS URLs and related properties')
parser.add_argument('sources',
                    metavar='sources',
                    type=str,
                    nargs='?',
                    help='path to sources directory',
                    default="sources")

args = parser.parse_args()
sources_directory = args.sources

response_cache = {}
domain_locks = {}
domain_lock = asyncio.Lock()

RequestResult = namedtuple('RequestResultCache',
                           ['status', 'text', 'exception'],
                           defaults=[None, None, None])


async def get_url(url: str, session: ClientSession, with_text=False, with_data=False, headers=None):
    """ Fetch url.
    This function ensures that only one request is sent to a domain at one point in time and that the same url is not
    queried more than once.
    Parameters
    ----------
    url : str
    session: ClientSession
    with_text: bool
    with_data : bool
    headers: dict
    Returns
    -------
    RequestResult
    """
    o = urlparse(url)
    if len(o.netloc) == 0:
        return RequestResult(exception="Could not parse URL: {}".format(url))

    async with domain_lock:
        if o.netloc not in domain_locks:
            domain_locks[o.netloc] = asyncio.Lock()
        lock = domain_locks[o.netloc]

    async with lock:
        if url not in response_cache:
            for i in range(2):
                try:
                    logging.debug("GET {}".format(url))
                    async with session.request(method="GET", url=url, ssl=False, headers=headers) as response:
                        status = response.status
                        if with_text:
                            try:
                                text = await response.text()
                            except:
                                text = await response.read()
                            response_cache[url] = RequestResult(status=status, text=text)
                        elif with_data:
                            data = await response.read()
                            response_cache[url] = RequestResult(status=status, text=data)
                        else:
                            response_cache[url] = RequestResult(status=status)
                except asyncio.TimeoutError:
                    response_cache[url] = RequestResult(exception="Timeout for: {}".format(url))
                except Exception as e:
                    logging.debug("Error for: {} ({})".format(url, str(e)))
                    response_cache[url] = RequestResult(exception="Exception {} for: {}".format(str(e), url))
                if RequestResult.exception is None:
                    break
        else:
            logging.debug("Cached {}".format(url))

        return response_cache[url]


def get_http_headers(source):
    """ Extract http headers from source"""
    headers = {}
    if 'custom-http-headers' in source['properties']:
        key = source['properties']['custom-http-headers']['header-name']
        value = source['properties']['custom-http-headers']['header-value']
        headers[key] = value
    return headers


async def process_source(filename, session):
    async with aiofiles.open(filename, mode='r', encoding='utf-8') as f:
        contents = await f.read()
    source = json.loads(contents)

    # Skip non tms layers
    if not source['properties']['type'] == 'tms':
        return
    if not 'country_code' in source['properties'] or not source['properties']['country_code'] == 'CZ':
        return
    if not ".zby.cz/tiles_cuzk.php" in source['properties']['url']:
        return

    tms_url = source['properties']['url']
    parameters = {}
    # {z} instead of {zoom}
    if '{z}' in tms_url:
        return
    if '{apikey}' in tms_url:
        return

    if "{switch:" in tms_url:
        match = re.search(r'switch:?([^}]*)', tms_url)
        switches = match.group(1).split(',')
        tms_url = tms_url.replace(match.group(0), 'switch')
        parameters['switch'] = switches[0]

    if 'geometry' in source and source['geometry'] is not None:
        geom = shape(source['geometry'])
        centroid = geom.representative_point()
    else:
        centroid = Point(0, 0)

    extra_headers = get_http_headers(source)

    tested_zooms = set()

    async def test_zoom(zoom):
        tested_zooms.add(zoom)
        tile = mercantile.tile(centroid.x, centroid.y, zoom)

        query_url = tms_url
        if '{-y}' in tms_url:
            y = 2 ** zoom - 1 - tile.y
            query_url = query_url.replace('{-y}', str(y))
        elif '{!y}' in tms_url:
            y = 2 ** (zoom - 1) - 1 - tile.y
            query_url = query_url.replace('{!y}', str(y))
        else:
            query_url = query_url.replace('{y}', str(tile.y))
        parameters['x'] = tile.x
        parameters['zoom'] = zoom
        query_url = query_url.format(**parameters)
        await asyncio.sleep(0.5)
        response = await get_url(query_url, session, with_data=True, headers=extra_headers)
        if response.status == 200:
            try:
                img = Image.open(BytesIO(response.text))
                image_hash = imagehash.average_hash(img)
                if str(image_hash) not in {'0000000000000000', 'FFFFFFFFFFFFFFFF'}:
                    return image_hash
            except Exception as e:
                logging.info("Error fetching image for {} : {} : {}".format(source['properties']['name'],
                                                                            query_url,
                                                                            str(e)))
        return None

    previous_images = []
    min_zoom = None
    for zoom in range(20):
        image_hash = await test_zoom(zoom)
        logging.info("{}: {}: {}".format(source['properties']['name'], zoom, image_hash))
        if image_hash is not None:
            # If previous image is the same as current, the zoom level is not useful
            if len(previous_images) > 0 and previous_images[-1] is not None and not previous_images[-1] == image_hash:
                min_zoom = len(previous_images)
                break
        previous_images.append(image_hash)

    # Check against source if we found at least one image
    if min_zoom is not None:

        # zoom 0 is equal to when no min_zoom level is set
        if min_zoom == 0:
            min_zoom = None

        original_min_zoom = None
        if 'min_zoom' in source['properties']:
            original_min_zoom = source['properties']['min_zoom']

        # Do nothing if exisiting value is same as tested value
        if not min_zoom == original_min_zoom:
            logging.info("Update {}: {}, previously: {}".format(source['properties']['name'],
                                                                min_zoom,
                                                                original_min_zoom))
            if min_zoom is None:
                source['properties'].pop('min_zoom', None)
            else:
                source['properties']['min_zoom'] = min_zoom

            with open(filename, 'w', encoding='utf-8') as out:
                json.dump(source, out, indent=4, sort_keys=False, ensure_ascii=False)
                out.write("\n")
    return


async def start_processing(sources_directory):
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSIE 6.0; ELI WMS sync )'}
    timeout = aiohttp.ClientTimeout(total=10)

    async with ClientSession(headers=headers, timeout=timeout) as session:
        jobs = []
        for filename in glob.glob(os.path.join(sources_directory, '**', '*.geojson'), recursive=True):
            jobs.append(process_source(filename, session))
        await asyncio.gather(*jobs)


asyncio.run(start_processing(sources_directory))
