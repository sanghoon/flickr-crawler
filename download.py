import hashlib
import json
import os
from glob import glob
from io import BytesIO
from urllib.error import HTTPError
from urllib.request import urlopen

import fire
import ray
import tqdm
from PIL import Image


class FlickrUrl:
    def __init__(self, img_id, owner=None, secret=None, server=None, farm=None,
                 title=None,
                 url_o=None, size_o=None):
        self.id = img_id
        self.owner = owner
        self.secret = secret
        self.server = server
        self.farm = farm
        self.title = title
        self.url_o = url_o
        self.size_o = size_o

        if secret is None or server is None:
            raise ValueError

    def to_dict(self):
        base_dict = {
            'id': self.id,
            'owner': self.owner,
            'secret': self.secret,
            'server': self.server,
            'farm': self.farm,
        }

        if self.title is not None:
            base_dict['title'] = self.title
        if self.url_o is not None:
            base_dict['url_o'] = self.url_o
        if self.size_o is not None:
            base_dict['size_o'] = self.size_o

        return base_dict

    @property
    def basename(self):
        return os.path.basename(self.get_url('o')).replace('_o.jpg', '.jpg')

    def get_url(self, size='b'):
        # See url specs at https://www.flickr.com/services/api/misc.urls.html
        """
        Always exist
        - s : 75x75 square
        - q : 150x150 square
        - m : 240-px
        - n : 320-px
        - z : 640-px
        - c : 800-px
        - b : 1024-px

        May not exist
        - 4k
        - o : the original photo
        """

        if size == 'o' and self.url_o is not None:
            return self.url_o
        if self.farm is not None:
            return f"https://farm{self.farm}.staticflickr.com/{self.server}/{self.id}_{self.secret}_{size}.jpg"

        raise NotImplementedError

    def __hash__(self):
        # Note: using hash(str) generates inconsistent hash values between processes
        return hash(self.id)

    def __eq__(self, other):
        # Note: ID is enough to compare two photos
        # (https://www.flickr.com/groups/51035612836@N01/discuss/72157594388157609/)
        if self.id != other.id:
            return False

        return True


@ray.remote
class SimpleDownloader:
    def __init__(self, output_dir='./', thumbs_dir=None):
        self.output_dir = output_dir
        self.thumbs_dir = thumbs_dir

    @staticmethod
    def download_image(furl, size='o'):
        try:
            url = furl.get_url(size)
            with urlopen(url) as f:
                content = f.read()

            return content
        except:
            return None

    def is_image_valid(self, img):
        if max(img.size) < 75:
            return False

        return True

    def try_download(self, furl, subdir=None):
        try:
            # Download a thumbnail and filter by its content
            url = furl.get_url('n')
            with urlopen(url) as f:
                thumb_content = f.read()
                thumb_im = Image.open(BytesIO(thumb_content))
                if not self.is_image_valid(thumb_im):
                    return None

            # Writeback a thumbnail if required
            if self.thumbs_dir is not None:
                if subdir is not None:
                    thumb_fname = os.path.join(self.thumbs_dir, subdir, furl.basename)
                else:
                    thumb_fname = os.path.join(self.thumbs_dir, furl.basename)
                os.makedirs(os.path.dirname(thumb_fname), exist_ok=True)
                with open(thumb_fname, 'wb') as f:
                    f.write(thumb_content)

            # Download a full-sized image
            full_url = furl.get_url('o')
            with urlopen(full_url) as f:
                content = f.read()
            if subdir is not None:
                image_fname = os.path.join(self.output_dir, subdir, furl.basename)
            else:
                image_fname = os.path.join(self.output_dir, furl.basename)
            os.makedirs(os.path.dirname(image_fname), exist_ok=True)
            with open(image_fname, 'wb') as f:
                f.write(content)

            # Additional metadata to be stored
            meta = {
                'img_path': os.path.relpath(image_fname, self.output_dir),
            }

            return furl, meta

        except HTTPError as e:
            # e.g.) Connection errors, 410: Gone, ...
            return None


def download_crawled_images(json_path='./crawled', output_path='./output', log_path=None,
                            create_subdirs=True, n_workers=4, max_images=-1):
    """
    Download crawled images
    :param json_path:  A path to a directory containing crawl results
    :param output_path: A path to store downloaded images
    :param log_path:
    :param create_subdirs:
    :param n_workers:
    :param max_images:
    :return:
    """
    json_files = sorted(glob(os.path.join(json_path, "**", "*.json"), recursive=True))

    if len(json_files) == 0:
        return

    # Initialize ray workers
    ray.init(num_cpus=n_workers, log_to_driver=False, local_mode=False)
    pool = ray.util.ActorPool([SimpleDownloader.remote(
        output_dir=os.path.join(output_path, 'images'),
        thumbs_dir=os.path.join(output_path, 'thumbs'),
    ) for _ in range(n_workers)])

    # Download all images
    queued_urls = set()
    for jsonfile in tqdm.tqdm(json_files, desc="Preparing"):
        with open(jsonfile, 'r') as f:
            search_results = json.load(f)

        for i, result in enumerate(search_results):
            if 'width_o' in result:
                size_o = (int(result['width_o']), int(result['height_o']))
            else:
                size_o = result.get['size_o']

            furl = FlickrUrl(result['id'], result['owner'], result['secret'],
                             result['server'], result['farm'],
                             title=result.get('title'),
                             url_o=result.get('url_o'), size_o=size_o)

            if furl in queued_urls:  # Duplicated search result
                continue

            queued_urls.add(furl)

            if create_subdirs:
                subdir_name = os.path.splitext(os.path.basename(jsonfile))[0]
                pool.submit(lambda actor, x: actor.try_download.remote(*x), (furl, subdir_name))
            else:
                pool.submit(lambda actor, x: actor.try_download.remote(x), furl)

        if len(queued_urls) > max_images > 0:
            break

    pbar = tqdm.tqdm(total=len(queued_urls), desc="Downloading")
    while pool.has_next():
        furl, meta = pool.get_next()
        pbar.update(1)

        # TODO: wrtieback meta


if __name__ == '__main__':
    fire.Fire(download_crawled_images)
