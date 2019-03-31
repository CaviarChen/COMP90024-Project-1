import argparse
import helper


def main() -> None:
    parser = argparse.ArgumentParser(usage='python src/main.py dataset/melbGrid.json dataset/tinyTwitter.json')
    parser.add_argument('melbgrid', type=str)
    parser.add_argument('twitter', type=str)

    args = parser.parse_args()
    melb_grid = helper.MelbGrid(args.melbgrid)

    counter = [helper.GridDataCounter() for _ in range(melb_grid.get_grid_num())]

    twitter_reader = helper.TwitterReader(args.twitter)

    while True:
        data = twitter_reader.read_one_twitter()
        if data is None:
            break
        (_, coord, hashtags) = data
        idx = melb_grid.find_grid_idx(coord[0], coord[1])
        if idx is not None:
            counter[idx].add_post()
            counter[idx].add_tags(hashtags)
    

if __name__ == "__main__":
    main()

