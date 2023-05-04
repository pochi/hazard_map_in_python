import os, sys
import argparse
import yaml
from xml.dom import minidom
from tqdm import tqdm
import requests
from multiprocessing import Pool, RLock
import numpy as np
import pathlib

def _parse(xml_file):
    title_and_identifier_and_tile = []
    dom = minidom.parse(xml_file)
    for layer in dom.getElementsByTagName("Layer"):
        title = layer.getElementsByTagName("ows:Title")[0]
        identifier = layer.getElementsByTagName("ows:Identifier")[0]
        tile_matrix = layer.getElementsByTagName("TileMatrixSet")[0]
        title_and_identifier_and_tile.append([
            title.firstChild.nodeValue,
            identifier.firstChild.nodeValue,
            tile_matrix.firstChild.nodeValue
        ])

    return title_and_identifier_and_tile

def _make_urls(tile):
    urls = []
    min_x = tile["min_x"]
    max_x = tile["max_x"]
    min_y = tile["min_y"]
    max_y = tile["max_y"]
    zoom_level = tile["zoom_level"]

    for i in tqdm(range(min_x, max_x)):
        for j in range(min_y, max_y):
            urls.append(str(tile["zoom_level"]) + "/" + str(i) + "/" + str(j))
            # この処理を入れると途中で止まる
            #url = base_url + str(tile["zoom_level"]) + "/" + str(i) + "/" + str(j) + ".png"
            #dirname = str(tile["zoom_level"]) + "/" + str(i) + "/" + str(j)
            #urls.append([dirname, url])

    return urls

def download_images(process_num, urls, output_dir):
    # title_and_identifier_and_tile = _parse(xml_file)
    base_url = 'https://disaportaldata.gsi.go.jp/raster/01_flood_l2_shinsuishin_kuni_data/'

    for prefix in tqdm(urls, desc=f'#{process_num:>2}', position=process_num+1):
        url = base_url + prefix
        response = requests.get(url)
        if response.status_code == 404:
            output_dir = output_dir + prefix
            os.makedirs(output_dir)
            pathlib.Path(os.path.join(output_dir, "404.txt")).touch()
            continue
            

        if response.status_code != 200:
            raise Exception("HTTP status " + str(response.status_code) + ": " + url)

        print(url)
        image = response.content

    return process_num


def fetch_hazard_map_images(config):
    xml_file = config["job"]["metadata_file"]
    # INFO: ここからAPIの定義がとれるが今回は一つしか使わないのでxmlは利用しない

    urls = _make_urls(config["job"]["tile"])
    process_num = os.cpu_count() - 1
    divide_urls = np.array_split(urls, process_num)
    divide_urls_with_process_nums = [(n, divide_urls[n], config["output"]["dir"]) for n in range(process_num)]
    with Pool(
        process_num,
        initializer=tqdm.set_lock,
        initargs=(RLock(), )
    ) as p:
        result = p.starmap(download_images, divide_urls_with_process_nums)

    return xml_file

if __name__ == "__main__":
    print("start hazard map client")
    parser = argparse.ArgumentParser(description="config for job")
    parser.add_argument("--c", dest="config_file", default="./config.yml")
    args = parser.parse_args()
    with open(args.config_file) as f:
        config = yaml.safe_load(f)

    print(config)

    fetch_hazard_map_images(config)


