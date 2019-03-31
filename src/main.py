from mpi4py import MPI
import argparse
import helper


def main() -> None:
    parser = argparse.ArgumentParser(usage='mpiexec -n 1 python src/main.py dataset/melbGrid.json dataset/tinyTwitter.json')
    parser.add_argument('melbgrid', type=str)
    parser.add_argument('twitter', type=str)

    args = parser.parse_args()
    melb_grid = helper.MelbGrid(args.melbgrid)

    counters = [helper.GridDataCounter() for _ in range(melb_grid.get_grid_num())]

    mpi_comm = MPI.COMM_WORLD
    node_num = mpi_comm.Get_size()
    node_rank = mpi_comm.Get_rank()

    # Stage 1: read file & count data separately
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

    # Stage 2: collect & merge data separately (each node will be assgined some grids)
    

    # Stage 3: the root node collects the results of different grid and output

if __name__ == "__main__":
    main()

