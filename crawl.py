import copy
import datetime
import os
import json
import time
import traceback
from enum import Enum

import fire
import flickrapi.exceptions
import tqdm
from flickrapi import FlickrAPI

from apikey import default_key, default_secret, flickr_api_url


class License(Enum):
    ALL_RESERVED = "0"
    CC_NC = "1,2,3"
    CC = "4,5,6"
    UNKNOWN = "7"
    OTHERS = "8,9,10"


API_PAGE_LIMIT = 500


def search_for_keyword(text, lic, target_date=None, target_days=1, max=4000,
                       apikey="", secret=""):
    # The following attributes will not be saved
    ATTRS_TO_SKIP = ['ispublic', 'isfamily', 'isfriend']

    assert max <= 4000, "Flickr API won't return more than 4000 results"

    # Check key existence
    if len(apikey) == 0:
        apikey = default_key
    if len(secret) == 0:
        secret = default_secret
    assert apikey and secret, f'Flickr API key is required, please visit {flickr_api_url}'

    flickr = FlickrAPI(apikey, secret)

    search_params = {}
    if target_date is not None:
        search_params['min_taken_date'] = target_date.strftime('%Y-%m-%d %H:%M:%S')
        search_params['max_taken_date'] = (target_date + datetime.timedelta(days=target_days)).strftime('%Y-%m-%d %H:%M:%S')

    # https://www.flickr.com/services/api/explore/?method=flickr.photos.licenses.getInfo
    photos = flickr.walk(text=text,  # http://www.flickr.com/services/api/flickr.photos.search.html
                         extras='url_o',
                         per_page=API_PAGE_LIMIT,  # 1-500
                         license=lic.value,
                         sort='relevance',
                         **search_params)

    results = []
    try:
        for i, photo in enumerate(photos):
            if max > 0 and i >= max:
                break

            try:
                url = photo.get('url_o')  # original size
                if url is None:
                    # Build a url (https://www.flickr.com/services/api/misc.urls.html)
                    url = 'https://farm%s.staticflickr.com/%s/%s_%s_b.jpg' % \
                          (photo.get('farm'), photo.get('server'), photo.get('id'), photo.get('secret'))  # large size

                this_result = copy.copy(photo.attrib)

                for k in ATTRS_TO_SKIP:
                    if k in this_result:
                        this_result.pop(k)

                this_result['url'] = url
                this_result['query_license'] = lic.name
                this_result['query_text'] = text
                results.append(this_result)
            except:
                pass
    except flickrapi.exceptions.FlickrError as e:
        raise e

    return results


def crawl_flickr_images(text='portrait', lic=License.CC,
                        date_from=datetime.date(2006, 1, 1),
                        date_until=datetime.date(2021, 12, 31),
                        date_interval=7,
                        query_delay=1,
                        output_dir=""):
    """
    Crawl Flickr image URLs
    :param text: search keyword
    :param lic: license (ALL_RESERVED, CC_NC, CC, UNKNOWN, OTHERS)
    :param date_from: search start date (2006.1.1 by default)
    :param date_until: search end date (2021.12.31 by default)
    :param date_interval:
    :param query_delay: a time delay between search queries (in seconds)
    :param output_dir: target directory to store crawl results
    :return:
    """
    # Argument handling
    if isinstance(lic, str):
        lic = License[lic.upper()]
    if not output_dir:
        output_dir = f"crawled/{text}"

    # Build list of search start date
    search_dates = []
    cur_date = date_from
    while cur_date <= date_until:
        search_dates.append(cur_date)
        cur_date = cur_date + datetime.timedelta(days=date_interval)

    # Query images and dump
    for _search_date in tqdm.tqdm(search_dates):
        try:
            results = search_for_keyword(text, lic,
                                         target_date=_search_date, target_days=date_interval)
        except flickrapi.exceptions.FlickrError as e:
            if e.code == 100:
                print("!!! Incorrect or missing API key.")
            else:
                print("!!! Unexpected error.")
                traceback.print_exc()
            return

        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, f"{_search_date.strftime('%Y%m%d')}_{len(results):04d}.json"), "w") as f:
            json.dump(results, f)

        if query_delay > 0:
            # Note: up to 1 call/s (https://www.flickr.com/services/developer/api/)
            time.sleep(query_delay)


if __name__ == '__main__':
    fire.Fire(crawl_flickr_images)
