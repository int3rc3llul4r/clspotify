import os
import time
from typing import Optional, Tuple

from librespot.metadata import EpisodeId

from const import ERROR, ID, ITEMS, NAME, SHOW, RELEASE_DATE, DURATION_MS, EXT_MAP
from termoutput import PrintChannel, Printer
from utils import create_download_directory, fix_filename
from track import convert_audio_format
from zspotify import ZSpotify
from loader import Loader


EPISODE_INFO_URL = 'https://api.spotify.com/v1/episodes'
SHOWS_URL = 'https://api.spotify.com/v1/shows'


def get_episode_info(episode_id_str) -> Tuple[Optional[str], Optional[str]]:
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching episode information..."):
        (raw, info) = ZSpotify.invoke_url(f'{EPISODE_INFO_URL}/{episode_id_str}')
    if not info:
        Printer.print(PrintChannel.ERRORS, "###   INVALID EPISODE ID   ###")
    duration_ms = info[DURATION_MS]
    if ERROR in info:
        return None, None
    return fix_filename(info[SHOW][NAME]), duration_ms,  fix_filename(info[NAME]), fix_filename(info[RELEASE_DATE])


def get_show_episodes(show_id_str) -> list:
    episodes = []
    offset = 0
    limit = 50

    with Loader(PrintChannel.PROGRESS_INFO, "Fetching episodes..."):
        while True:
            resp = ZSpotify.invoke_url_with_params(
                f'{SHOWS_URL}/{show_id_str}/episodes', limit=limit, offset=offset)
            offset += limit
            for episode in resp[ITEMS]:
                episodes.append(episode[ID])
            if len(resp[ITEMS]) < limit:
                break

    return episodes


def download_podcast_directly(url, filename):
    import functools
    import pathlib
    import shutil
    import requests
    from tqdm.auto import tqdm

    r = requests.get(url, stream=True, allow_redirects=True)
    if r.status_code != 200:
        r.raise_for_status()  # Will only raise for 4xx codes, so...
        raise RuntimeError(
            f"Request to {url} returned status code {r.status_code}")
    file_size = int(r.headers.get('Content-Length', 0))

    path = pathlib.Path(filename).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    desc = "(Unknown total file size)" if file_size == 0 else ""
    r.raw.read = functools.partial(
        r.raw.read, decode_content=True)  # Decompress if needed
    with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
        with path.open("wb") as f:
            shutil.copyfileobj(r_raw, f)

    return path


def download_episode(episode_id) -> None:
    podcast_name, duration_ms, episode_name, release_date = get_episode_info(episode_id)
    prepare_download_loader = Loader(PrintChannel.PROGRESS_INFO, "Preparing download...")
    prepare_download_loader.start()

    if podcast_name is None:
        Printer.print(PrintChannel.SKIPS, '###   SKIPPING: (EPISODE NOT FOUND)   ###')
        prepare_download_loader.stop()
    else:
        ext = EXT_MAP.get(ZSpotify.CONFIG.get_download_format().lower())

        output_template = ZSpotify.CONFIG.get_output('podcast')

        output_template = output_template.replace("{podcast}", fix_filename(podcast_name))
        output_template = output_template.replace("{episode_name}", fix_filename(episode_name))
        output_template = output_template.replace("{release_date}", fix_filename(release_date))
        output_template = output_template.replace("{ext}", fix_filename(ext))

        filename = os.path.join(ZSpotify.CONFIG.get_root_podcast_path(), output_template)
        download_directory = os.path.dirname(filename)
        create_download_directory(download_directory)

        episode_id = EpisodeId.from_base62(episode_id)
        stream = ZSpotify.get_content_stream(
            episode_id, ZSpotify.DOWNLOAD_QUALITY)

        total_size = stream.input_stream.size

        if (
            os.path.isfile(filename)
            and os.path.getsize(filename) == total_size
            and ZSpotify.CONFIG.get_skip_existing_files()
        ):
            Printer.print(PrintChannel.SKIPS, "\n###   SKIPPING: " + podcast_name + " - " + episode_name + " (EPISODE ALREADY EXISTS)   ###")
            prepare_download_loader.stop()
            return

        prepare_download_loader.stop()
        time_start = time.time()
        downloaded = 0
        with open(filename, 'wb') as file, Printer.progress(
            desc=filename,
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024
        ) as p_bar:
            prepare_download_loader.stop()
            while total_size > downloaded:
                data = stream.input_stream.stream().read(ZSpotify.CONFIG.get_chunk_size())
                p_bar.update(file.write(data))
                downloaded += len(data)
                if len(data) == 0:
                    break
                if ZSpotify.CONFIG.get_download_real_time():
                    delta_real = time.time() - time_start
                    delta_want = (downloaded / total_size) * (duration_ms/1000)
                    if delta_want > delta_real:
                        time.sleep(delta_want - delta_real)
            
            convert_audio_format(filename)

    prepare_download_loader.stop()
