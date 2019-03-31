from mpi4py import MPI
from typing import *
import argparse
import helper


def main() -> None:
    parser = argparse.ArgumentParser(usage='mpiexec -n 1 python src/main.py dataset/melbGrid.json dataset/tinyTwitter.json')
    parser.add_argument('melbgrid', type=str)
    parser.add_argument('twitter', type=str)

    args = parser.parse_args()
    melb_grid = helper.MelbGrid(args.melbgrid)
    grid_num = melb_grid.get_grid_num()

    counters: List[Optional[helper.GridDataCounter]] = [helper.GridDataCounter() for _ in range(grid_num)]

    mpi_comm = MPI.COMM_WORLD
    node_num = mpi_comm.Get_size()
    node_rank = mpi_comm.Get_rank()

    # ----
    # Stage 1: read file & count data separately
    # ----
    twitter_reader = helper.TwitterReader(args.twitter, node_num, node_rank)
    while True:
        data = twitter_reader.read_one_twitter()
        if data is None:
            break
        (_, coord, hashtags) = data
        idx = melb_grid.find_grid_idx(coord[0], coord[1])
        if idx is not None:
            counters[idx].add_post()
            counters[idx].add_tags(hashtags)
    del twitter_reader
    # print([c._post_count for c in counters])

    # ----
    # Stage 2: collect & merge data separately (each node will be assgined some grids)
    # ----
    # gather data
    gathered_data: List[Optional[List[Any]]] = [None for _ in range(grid_num)]
    for i in range(grid_num):
        # node with rank == assigned_node shoule handled this grid
        assigned_node = i % node_num
        gathered_data[i] = mpi_comm.gather(counters[i].marshal_data(), root = assigned_node)
        if assigned_node != node_rank:
            # not assigned to this node, free counter
            counters[i] = None
    
    # merge data
    results: List[Any] = [None for _ in range(grid_num)]
    for i in range(grid_num):
        if counters[i] is not None:
            for d in gathered_data[i]:
                (post_num, tag_list) = d
                counters[i].add_post(post_num)
                counters[i].merge_tags(tag_list)
                # top 5 tags
                results[i] = counters[i].get_result(5)

    # ----
    # Stage 3: the root node collects the results of different grid and output
    # ----
    if node_rank == 0:
        final_results = [None for _ in range(grid_num)]

    for i in range(grid_num):
        assigned_node = i % node_num
        # data on current node, no need to send/recv data
        if assigned_node == 0 and node_rank == 0:
            final_results[i] = results[i]
            continue

        if assigned_node == node_rank:
            mpi_comm.send(results[i], dest=0, tag=i)
            continue

        if node_rank == 0:
            final_results[i] = mpi_comm.recv(source=assigned_node, tag=i)
            continue
    del results
    
    if node_rank == 0:
        print(final_results)

if __name__ == "__main__":
    main()

