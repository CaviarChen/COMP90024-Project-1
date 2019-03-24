from typing import *
import json


class MelbGrid:
    def __init__(self, file_name: str) -> None:
        with open(file_name, 'r') as file:
            data = json.load(file)

        self._melb_grids: List[Dict] = []

        for line in data['features']:
            grid = {'id': line['properties']['id'],
                    'xmin': line['properties']['xmin'],
                    'xmax': line['properties']['xmax'],
                    'ymin': line['properties']['ymin'],
                    'ymax': line['properties']['ymax']}
            self._melb_grids.append(grid)

    def find_grid_idx(self, x: float, y: float) -> Optional[int]:
        for idx, grid in enumerate(self._melb_grids):
            if grid['xmin'] <= x <= grid['xmax'] and grid['ymin'] <= y <= grid['ymax']:
                return idx
        return None

    def grid_idx_to_id(self, idx: int) -> str:
        return self._melb_grids[idx]['id']


class TwitterReader:
    def __init__(self, file_name: str) -> None:
        self._file = open(file_name, 'r')
        # ignore the first line (eg. "{"total_rows":3877777,"offset":805584,"rows":[")
        self._file.readline()
        self._line_count = 0
        self._line_skip_num = 1
        self._line_skip_offset = 0
    
    def set_line_skip(self, num: int, offset: int) -> None:
        self._line_skip_num = num
        self._line_skip_offset = offset

    def read_one_twitter(self) -> Optional[Tuple[int, List[float], List[str]]]:
        while self._line_count % self._line_skip_num != self._line_skip_offset:
            self._file.readline()
            self._line_count += 1
        
        line: str = self._file.readline()
        # check end of file
        if line == '' or line.strip() == ']}':
            return None
        
        # parse row
        data = json.loads(line.rstrip(",\r\n "))
        coord = list(map(float, data['value']['geometry']['coordinates']))
        hashtags = [str(item['text']) for item in data['doc']['entities']['hashtags']]

        return (self._line_count, coord, hashtags)

    def __del__(self) -> None:
        self._file.close()