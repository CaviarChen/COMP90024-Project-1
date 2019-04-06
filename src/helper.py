from typing import *
import heapq
import json
import os
import time


class MelbGrid:
    """MelbGrid stores the information about melbourne grid.

    Attributes:
        _melb_grids: list of dictionaries of each grid with id, longitude and latitude.
    """
    def __init__(self, file_name: str) -> None:
        """Initialization. Read the grid file, and store it.

        Args:
            file_name (str): file name of grid data.
        """
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
        # sort grids by latitude descendingly and longitude ascendingly
        self._melb_grids = sorted(self._melb_grids, key=lambda e: (-e['ymin'], e['xmin']))

    def find_grid_idx(self, x: float, y: float) -> Optional[int]:
        """Find the grid index by its coordinate.

        Args:
            x (float): longitude coordinate.
            y (float): latitude coordinate.

        Returns:
            int: grid index, none if not found.
        """
        for idx, grid in enumerate(self._melb_grids):
            if grid['xmin'] <= x <= grid['xmax'] and grid['ymin'] <= y <= grid['ymax']:
                return idx
        return None

    def grid_idx_to_id(self, idx: int) -> str:
        """Find the id by the given grid index.

        Args:
            idx (int): index in grid list.

        Returns:
            str: corresponding grid id.
        """
        return self._melb_grids[idx]['id']

    def get_grid_num(self) -> int:
        """Return the number of stored grids.

        Returns:
            int: the number of grids.
        """
        return len(self._melb_grids)


class TwitterReader:
    """TwitterReader is a helper class for reading the twitter.

    Attribtues:
        _file_: file used to read.
        _line_count: line count of line.
        _file_start: start position of file.
        _file_end: end position of file.
    """
    def __init__(self, file_name: str, node_num: int, node_rank: int) -> None:
        """Initialization.

        Args:
            file_name (str): file name of twitter data.
            node_num (int): the total number of nodes.
            node_rank (int): the rank of current node.
        """
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
        """Align to avoid the newline or EOF.

        Args:
            rough_pos(int): rough position for offset.

        Returns:
            int: optimized position.
        """
        self._file.seek(rough_pos)
        # keep read util reach a newline or EOF
        while self._file.read(1) not in ['', '\n']:
            pass
        return self._file.tell()

    def read_one_twitter(self) -> Optional[Tuple[int, List[float], List[str]]]:
        """Read one twitter data to extract the data.

        Returns:
            (int, [float, [str]]): line count and list of coordinate and its hashtag list.
        """
        line = self._file.readline()
        # stop if reach the last line (eg. ']}') or reachs out of the chunk that assigned to this node
        if self._file.tell() > self._file_end or line.strip() == ']}':
            return None

        # parse row
        try:
            data = json.loads(line.rstrip(",\r\n "))
            coord = list(map(float, data['doc']['coordinates']['coordinates']))
            # check that coordinates have two values
            assert len(coord) == 2
            hashtags = [str(item['text']).lower() for item in data['doc']['entities']['hashtags']]
            # remove duplicates
            hashtags = list(set(hashtags))
        except Exception as e:
            # parsing error
            # print("ERRPR:\n", e)
            return (self._line_count, [], [])

        return (self._line_count, coord, hashtags)

    def __del__(self) -> None:
        """Close the file.
        """
        self._file.close()


class GridDataCounter:
    """GridDataCounter stored the specific results for grid.

    Attributes:
        _post_count (int): post count for grid.
        _tag_dict {str: int}: dictionary of tags with its occurrence.
    """
    def __init__(self) -> None:
        """Initialization.
        """
        self._post_count = 0
        self._tag_dict: Dict[str, int] = {}

    def add_post(self, num: int = 1) -> None:
        """Add the given number into post count attribute.

        Args:
            num (int): default is 1.
        """
        self._post_count += num

    def add_tag(self, tag: str) -> None:
        """Add the occurrence of the given tag.

        Args:
            tag (str): tag.
        """
        if tag in self._tag_dict:
            self._tag_dict[tag] += 1
        else:
            self._tag_dict[tag] = 1

    def add_tags(self, tags: List[str]) -> None:
        """Add the occurrence of all the present tags.

        Args:
            tags ([str]): list of tags.
        """
        for t in tags:
            self.add_tag(t)

    def merge_tags(self, tags: List[Tuple[str, int]]) -> None:
        """Merge all the present tags into stored tag dictionary.

        Args:
            tags ([(str, int)]): list of tag and its count.

        """
        for tag in tags:
            if tag[0] in self._tag_dict:
                self._tag_dict[tag[0]] += tag[1]
            else:
                self._tag_dict[tag[0]] = tag[1]

    def marshal_data(self) -> Tuple[int, List[Tuple[str, int]]]:
        """Marshal the data into optimized type.

        Returns:
            (int, [(str, int)]): post count and results of hashtag and count.
        """
        return (self._post_count, list(self._tag_dict.items()))

    def get_result(self, n: int) -> Tuple[int, List[Tuple[str, int]]]:
        """Get the top n results by distinct values.

        Args:
            n (int): top count.

        Returns:
            (int, [(str, int)]): post count and top n results of hashtag and count.
        """
        topk_freq = heapq.nlargest(n, set(self._tag_dict.values()))
        if len(topk_freq) < n:
            # No enought tags, take everything
            tags = list(self._tag_dict.items())
        else:
            tags = list(filter(lambda x: x[1] >= topk_freq[-1], self._tag_dict.items()))

        return (self._post_count, sorted(tags, key=lambda x: -x[1]))


class Timer:
    """Timer is used to count the stage time.

    Attributes:
        _stage_time ([(float, str)]): stored stage time with each given string.
    """
    def __init__(self) -> None:
        """Initialization, add the start time in advance.
        """
        self._stage_time: List[Tuple[float, str]] = []
        self.add_stage_time("")

    def add_stage_time(self, name: str) -> None:
        """Add one stage time period.

        Args:
            name (str): specific name for this time period.
        """
        self._stage_time.append((time.time(), name))

    def print_result(self) -> None:
        """Print the stage time results in specific format.
        """
        total_time: float = 0
        for i in range(1, len(self._stage_time)):
            time_used = self._stage_time[i][0] - self._stage_time[i-1][0]
            total_time += time_used
            print("Stage {} is executed in {:.5f} seconds".format(self._stage_time[i][1], time_used))

        print("Total execution time is {:.5f} seconds".format(total_time))
