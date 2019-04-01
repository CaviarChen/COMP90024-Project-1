from typing import *
import heapq
import json
import os
import time


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
        # sort melb grid by ymin and xmin values
        self._melb_grids = sorted(self._melb_grids, key=lambda e: (-e['ymin'], e['xmin']))

    def find_grid_idx(self, x: float, y: float) -> Optional[int]:
        for idx, grid in enumerate(self._melb_grids):
            if grid['xmin'] <= x <= grid['xmax'] and grid['ymin'] <= y <= grid['ymax']:
                return idx
        return None

    def grid_idx_to_id(self, idx: int) -> str:
        return self._melb_grids[idx]['id']
    
    def get_grid_num(self) -> int:
        return len(self._melb_grids)


class TwitterReader:
    def __init__(self, file_name: str, node_num: int, node_rank: int) -> None:
        self._file = open(file_name, 'r')
        self._line_count = 0

        # divide file
        file_size = os.fstat(self._file.fileno()).st_size
        chunk_size: float = file_size / node_num
        # divide file roughly based on pos
        rough_start = int(chunk_size * node_rank)
        rough_end = int(chunk_size * (node_rank + 1))
        # align to the next newline
        # no need to worry about the first line since we need to ignore it anyway
        # (eg. "{"total_rows":3877777,"offset":805584,"rows":[")
        self._file_start = self._align_to_newline(rough_start)
        self._file_end = self._align_to_newline(rough_end)
        # move to the right pos
        self._file.seek(self._file_start)
        
    def _align_to_newline(self, rough_pos: int) -> int:
        self._file.seek(rough_pos)
        # keep read util reach a newline or EOF
        while self._file.read(1) not in ['', '\n']:
            pass
        return self._file.tell()

    def read_one_twitter(self) -> Optional[Tuple[int, List[float], List[str]]]:
        line = self._file.readline()
        # stop if reach the last line (eg. ']}') or reachs out of the chunk that assigned to this node
        if self._file.tell() > self._file_end or line.strip() == ']}':
            return None
        
        # parse row
        try:
            data = json.loads(line.rstrip(",\r\n "))            
            coord = list(map(float, data['value']['geometry']['coordinates']))
            # check that coordinates have two values
            assert len(coord) == 2
            hashtags = [str(item['text']).lower() for item in data['doc']['entities']['hashtags']]
            # remove duplicates
            hashtags = list(set(hashtags))
        except Exception as e:
            # parsing error
            print("ERRPR:\n", e)
            return (self._line_count, [], [])

        return (self._line_count, coord, hashtags)

    def __del__(self) -> None:
        self._file.close()

class GridDataCounter:
    def __init__(self) -> None:
        self._post_count = 0
        self._tag_dict: Dict[str, int] = {}

    def add_post(self, num: int = 1) -> None:
        self._post_count += num
    
    def add_tag(self, tag: str) -> None:
        if tag in self._tag_dict:
            self._tag_dict[tag] += 1
        else:
            self._tag_dict[tag] = 1

    def add_tags(self, tags: List[str]) -> None:
        for t in tags:
            self.add_tag(t)
    
    def merge_tags(self, tags: List[Tuple[str, int]]) -> None:
        for tag in tags:
            if tag[0] in self._tag_dict:
                self._tag_dict[tag[0]] += tag[1]
            else:
                self._tag_dict[tag[0]] = tag[1]

    def marshal_data(self) -> Tuple[int, List[Tuple[str, int]]]:
        return (self._post_count, list(self._tag_dict.items()))

    def get_result(self, n: int) -> Tuple[int, List[Tuple[str, int]]]:
        tags = heapq.nlargest(n, self._tag_dict.items(), key= lambda x: x[1])
        return (self._post_count, tags)

class TimeCounter:
    def __init__(self) -> None:
        self.start_time = time.time()
        self.stage_time: List[float] = [None, None, None]
    
    def add_stage_time(self, i: int) -> None:
        stage_time = time.time()
        if i == 1:
            self.stage_time[i-1] = stage_time - self.start_time
        else:
            self.stage_time[i-1] = stage_time - self.start_time - self.stage_time[i-2]
        
    def print_stage_time(self) -> List[float]:
        for i in range(len(self.stage_time)):
            print("Stage {} is executed in {:.5f} seconds".format(i + 1, self.stage_time[i]))
        print("Total execution time is {:.5f} seconds".format(sum(self.stage_time)))