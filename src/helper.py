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

