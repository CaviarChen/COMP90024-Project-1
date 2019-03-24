import argparse
import helper


def main() -> None:
    parser = argparse.ArgumentParser(usage='python src/main.py dataset/melbGrid.json dataset/tinyTwitter.json')
    parser.add_argument('melbgrid', type=str)
    parser.add_argument('twitter', type=str)

    args = parser.parse_args()
    melb_grid = helper.MelbGrid(args.melbgrid)

    twitter_reader = helper.TwitterReader(args.twitter)

    for i in range(100):
        print(twitter_reader.read_one_twitter())

if __name__ == "__main__":
    main()

