import argparse
import asyncio
import glob
import json
import logging
import os
import re
from collections import namedtuple
import aiohttp
import mercantile
from shapely.geometry import shape, Point
import aiofiles
from aiohttp import ClientSession
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from PIL import Image
from io import BytesIO

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='Check if tms sources support highres tiles')
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
            for i in range(3):
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


async def process_source(filename, session: ClientSession):
    async with aiofiles.open(filename, mode='r', encoding='utf-8') as f:
        contents = await f.read()
        source = json.loads(contents)

    if not source['properties']['type'] == 'tms':
        return
    if 'header' in source['properties']['url']:
        return
    if 'geometry' in source and source['geometry'] is not None:
        geom = shape(source['geometry'])
        centroid = geom.representative_point()
    else:
        centroid = Point(6.1, 49.6)

    tms_url = source['properties']['url']

    parameters = {}
    # {z} instead of {zoom}
    if '{z}' in source['properties']['url']:
        return
    if '{apikey}' in tms_url:
        return
    if "{switch:" in tms_url:
        match = re.search(r'switch:?([^}]*)', tms_url)
        switches = match.group(1).split(',')
        tms_url = tms_url.replace(match.group(0), 'switch')
        parameters['switch'] = switches[0]

    min_zoom = 0
    max_zoom = 22
    if 'min_zoom' in source['properties']:
        min_zoom = int(source['properties']['min_zoom'])
    if 'max_zoom' in source['properties']:
        max_zoom = int(source['properties']['max_zoom'])

    async def test_zoom(zoom, highres=False):
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

        if highres:
            u = urlparse(query_url)
            url_parts = list(u)
            exts = {'png', 'jpg', 'jpeg', 'png32', 'png64', 'png128', 'png256', 'jpg70', 'jpg80', 'jpg90'}
            for ext in exts:
                if ".{}".format(ext) in u.path:
                    url_parts[2] = url_parts[2].replace(".{}".format(ext), "@2x.{}".format(ext))
                    break
            else:
                url_parts[2] += "@2x"
            query_url = urlunparse(url_parts)

        query_url = query_url.format(**parameters)
        respone = await get_url(query_url, session, with_data=True)
        if respone.status == 200:
            try:
                img = Image.open(BytesIO(respone.text))
                if highres and img.size == (512, 512):
                    return True
                elif not highres and img.size == (256, 256):
                    return True
            except Exception as e:
                print(e)
        return False

    zoom = int(0.5 * (min_zoom + max_zoom))

    if await test_zoom(zoom, highres=False) and await test_zoom(zoom, highres=True):
        logging.info("Supports @2x: {}".format(source['properties']['id']))


async def start_processing(sources_directory):
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSIE 6.0; ELI highres tile check)'}
    timeout = aiohttp.ClientTimeout(total=10)

    async with ClientSession(headers=headers, timeout=timeout) as session:
        jobs = []
        for filename in glob.glob(os.path.join(sources_directory, '**', '*.geojson'), recursive=True):
            jobs.append(process_source(filename, session))
        await asyncio.gather(*jobs)


asyncio.run(start_processing(sources_directory))
