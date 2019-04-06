from mpi4py import MPI
from typing import *
import argparse
import datetime
import helper


def main() -> None:
    """Main function is used to execute the task.
    """
    # Parser usage
    parser = argparse.ArgumentParser(usage='mpiexec -n 1 python src/main.py dataset/melbGrid.json dataset/tinyTwitter.json')
    parser.add_argument('melbgrid', type=str)
    parser.add_argument('twitter', type=str)

    args = parser.parse_args()

    task = Task(args.melbgrid, args.twitter, MPI.COMM_WORLD)
    task.execute()


class Task:
    """Task class is used to run all the stages for given dataset.

    Attributes:
        _grid_file (str): file of grid data.
        _twitter_file (str): file of twitter data.
        _mpi_comm: MPI.COMM_WORLD.
        _node_num: the total number of nodes.
        _node_rank: the rank of current node.
    """
    def __init__(self, grid_file: str, twitter_file: str, mpi_comm: MPI.Intracomm) -> None:
        """Initialization.

        Args:
            grid_file (str): file of grid data.
            twitter_file (str): file of twitter data.
            mpi_comm (MPI.Intracomm): MPI.COMM_WORLD.
        """
        self._grid_file = grid_file
        self._twitter_file = twitter_file
        self._mpi_comm = mpi_comm
        self._node_num = mpi_comm.Get_size()
        self._node_rank = mpi_comm.Get_rank()

        # root node
        if self._node_rank == 0:
            self._print_title("Task")
            print("task_time:   ", datetime.datetime.now())
            print("grid_file:   ", grid_file)
            print("twitter_file:", twitter_file)
            print("node_num:    ", self._node_num)
            print()

    def execute(self) -> None:
        """Execute the task.
        """
        # Start timer
        if self._node_rank == 0:
            timer = helper.Timer()

        # stage 1
        (melb_grid, counters) = self._stage1()
        if self._node_rank == 0:
            timer.add_stage_time("1 ")

        # stage 2a
        gathered_data = self._stage2a(counters, melb_grid.get_grid_num())
        del counters
        if self._node_rank == 0:
            timer.add_stage_time("2a")

        # stage 2b
        results = self._stage2b(gathered_data, melb_grid.get_grid_num())
        del gathered_data
        if self._node_rank == 0:
            timer.add_stage_time("2b")

        # stage 3
        self._stage3(melb_grid, results)
        del results
        if self._node_rank == 0:
            timer.add_stage_time("3 ")
            self._print_title("Timer")
            timer.print_result()

    def _stage1(self) -> Tuple[helper.MelbGrid, List[helper.GridDataCounter]]:
        """
        Stage 1: load & count data
        - Load the melbourne grid file.
        - Read a part of the twitter file & count posts and hashtags.
        - No communication between nodes in this stage.

        Returns:
            [GridDataCounter]: list of GridDataCounter for all grids.
        """
        melb_grid = helper.MelbGrid(self._grid_file)
        grid_num = melb_grid.get_grid_num()

        counters: List[helper.GridDataCounter] = [helper.GridDataCounter() for _ in range(grid_num)]

        twitter_reader = helper.TwitterReader(self._twitter_file, self._node_num, self._node_rank)
        while True:
            data = twitter_reader.read_one_twitter()
            if data is None:
                break
            (_, coord, hashtags) = data
            # skip when the data is not right
            if len(coord) != 2:
                continue
            idx = melb_grid.find_grid_idx(coord[0], coord[1])
            if idx is not None:
                counters[idx].add_post()
                counters[idx].add_tags(hashtags)
        del twitter_reader

        return (melb_grid, counters)

    def _stage2a(self, counters: List[Optional[helper.GridDataCounter]], grid_num: int) -> List[Optional[List[Any]]]:
        """
        Stage 2a: collect data of each grid
        - Grid with id i will be handled on node (i % node_num),
        so that node needs to collect data of grid i from all other nodes.

        Args:
            counters ([GridDataCounter]): list of GridDataCounter for all grids.
            grid_num (int): the number of grids.

        Returns:
            [[(str, int)]]: list of all executed data for all grids.
        """
        gathered_data: List[Optional[List[Any]]] = [None for _ in range(grid_num)]
        for i in range(grid_num):
            # node with rank == assigned_node shoule handled this grid
            assigned_node = i % self._node_num
            gathered_data[i] = self._mpi_comm.gather(counters[i].marshal_data(), root=assigned_node)
            if assigned_node != self._node_rank:
                # not assigned to this node, free counter
                counters[i] = None
        return gathered_data

    def _stage2b(self, gathered_data: List[Optional[List[Any]]], grid_num: int) -> List[Any]:
        """
        Stage 2b: merge data of each grid
        - Grid with id i will be handled on node (i % node_num).
        - No communication between nodes in this stage.

        Args:
            gathered_data ([[(str, int)]]): list of all executed data for all grids.
            grid_num (int): the number of grids.

        Returns:
            [(int, [(str, int)])]: list of tuple of post count and list of its hashtag and count.
        """
        results: List[Any] = [None for _ in range(grid_num)]
        for i in range(grid_num):
            if gathered_data[i] is not None:
                counter = helper.GridDataCounter()
                for d in gathered_data[i]:
                    (post_num, tag_list) = d
                    counter.add_post(post_num)
                    counter.merge_tags(tag_list)
                # top 5 tags
                results[i] = counter.get_result(5)
        return results

    def _stage3(self, melb_grid: helper.MelbGrid, results: List[Any]) -> None:
        """
        Stage 3: final output
        - The root node collects the results from all other nodes and output.

        Args:
            melb_grid (MelbGrid): MelbGrid class.
            results ([(int, [(str, int)])]): list of tuple of post count and list of its hashtag and count.
        """
        grid_num = melb_grid.get_grid_num()
        if self._node_rank == 0:
            final_results: List[Optional[Any]] = [None for _ in range(grid_num)]

        for i in range(grid_num):
            assigned_node = i % self._node_num
            # data on current node, no need to send/recv data
            if assigned_node == 0 and self._node_rank == 0:
                final_results[i] = results[i]
                continue

            # nodes except the root node will send data to root node.
            if assigned_node == self._node_rank:
                self._mpi_comm.send(results[i], dest=0, tag=i)
                continue

            # root node will receive all the data.
            if self._node_rank == 0:
                final_results[i] = self._mpi_comm.recv(source=assigned_node, tag=i)
                continue
        del results

        # root node will be responsible for the final output.
        if self._node_rank == 0:
            post_list = []
            hashtag_list = []
            for i in range(len(final_results)):
                post_list.append((i, final_results[i][0]))
                hashtag_list.append(final_results[i][1])

            post_list.sort(key=lambda x: x[1], reverse=True)

            # Output the count and top 5 hashtags in each grid cell
            self._print_title("Result")
            for post_i in post_list:
                print("{}: {} posts {}".format(melb_grid.grid_idx_to_id(post_i[0]), post_i[1], tuple(hashtag_list[post_i[0]])))

    def _print_title(self, title: str) -> None:
        print()
        print(' ', title)
        print('-'*20)


if __name__ == "__main__":
    main()
