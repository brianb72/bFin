from pathlib import Path

QUANDL_KEY_PATH = Path('~/.quandlkey').expanduser()
CACHE_DIR = Path('~/.bfincache').expanduser()


#if not CACHE_DIR.is_dir():
#    print(f'Cache dir {CACHE_DIR} not found, create to enable cache.')
#else:
#    print(f'Using cache dir {CACHE_DIR}')