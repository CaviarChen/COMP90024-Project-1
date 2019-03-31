import argparse
import helper


def main() -> None:
    parser = argparse.ArgumentParser(usage='python src/main.py dataset/melbGrid.json dataset/tinyTwitter.json')
    parser.add_argument('melbgrid', type=str)
    parser.add_argument('twitter', type=str)

    args = parser.parse_args()
    melb_grid = helper.MelbGrid(args.melbgrid)

    counters = [helper.GridDataCounter() for _ in range(melb_grid.get_grid_num())]

    twitter_reader = helper.TwitterReader(args.twitter, 1, 0)

    while True:
        data = twitter_reader.read_one_twitter()
        if data is None:
            break
        (_, coord, hashtags) = data
        idx = melb_grid.find_grid_idx(coord[0], coord[1])
        if idx is not None:
            counters[idx].add_post()
            counters[idx].add_tags(hashtags)
    
    print([c._post_count for c in counters])
    

if __name__ == "__main__":
    main()

