# Folder-synchronization.
# Program maintain a full, identical copy of source folder at replica folder.
from argparse import ArgumentParser
from time import sleep
import logging
from logging.handlers import RotatingFileHandler
import os
import hashlib
import shutil


log = logging.getLogger(__name__)
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)


def delete_files(files_set: set, path_dict: dict) -> None:
    """
    delete all files from files_set
    :type files_set: object
    :param files_set: set
    :param path_dict: dict
    :return: None
    """
    for md_file in files_set:
        path = path_dict.get(md_file)
        try:
            os.remove(path)
        except Exception as e:
            log.error('Delete file error %s', e)
        else:
            log.info(f'File {path} deleted')


def copy_files(files_set: set, path_dict: dict, src: str, dst: str) -> None:
    """
    Copy all files_set files from src to dst
    :type files_set: object
    :param files_set: dict
    :param path_dict: dict
    :param src: str
    :param dst: str
    :return: None
    """
    for md_file in files_set:
        source_path = path_dict.get(md_file)
        destination_path = path_dict.get(md_file).replace(src, dst)

        destination_dir = '/'.join(destination_path.split('/')[:-1])
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
        try:
            shutil.copy2(src=source_path, dst=destination_path)
        except Exception as e:
            log.error('Copy file error %s', e)
        else:
            log.info(f'File {destination_path} copied!')


def md5(absolute_path: str, local_path: str) -> str:
    """
    return md5 hash of file by file name
    :param local_path: str
    :param absolute_path: str
    :return: str
    """
    hash_md5 = hashlib.md5(bytes(local_path, 'utf-8'))
    with open(absolute_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def find_files(source_path, src):
    """
    create dict with md5 as key and path as value
    :param src: str
    :param source_path: str
    :return: dict
    """
    result = {}
    for filename in os.listdir(source_path):
        path = source_path + '/' + filename
        if os.path.isdir(path):
            result.update(find_files(path, src))
        else:
            local_path = path.replace(src, '')
            result.update({md5(path, local_path): path})
    return result


def parse_program_args(parser: ArgumentParser) -> tuple:
    """
    parse all args for program
    :param parser:
    :return: tuple
    """
    parser.add_argument('-s', '--source', type=str, required=True)
    parser.add_argument('-d', '--destination', type=str, required=True)
    parser.add_argument('-t', '--time', type=int, required=True)
    parser.add_argument('-l', '--log', type=str, required=True)
    args = parser.parse_args()
    handler = RotatingFileHandler(args.log, maxBytes=1000000, backupCount=10)
    handler.setFormatter(logging.Formatter(log_format))
    log.addHandler(handler)
    return args.source, args.destination, args.time


if __name__ == '__main__':
    source, destination, d_time = parse_program_args(ArgumentParser())

    if not os.path.exists(destination):
        os.mkdir(destination)

    while True:
        log.info('Start synchronization')

        source_files = find_files(source, source)
        destination_files = find_files(destination, destination)
        source_files_keys = set(source_files.keys())
        destination_files_keys = set(destination_files.keys())

        delete_files(destination_files_keys - source_files_keys, destination_files)
        copy_files(source_files_keys - destination_files_keys, source_files, source, destination)

        log.info("End synchronization, wait %s", d_time)
        sleep(d_time)
