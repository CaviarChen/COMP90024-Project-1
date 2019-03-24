import argparse
import helper


def main() -> None:
    parser = argparse.ArgumentParser(usage='python src/main.py dataset/melbGrid.json dataset/tinyTwitter.json')
    parser.add_argument('melbgrid', type=str)
    parser.add_argument('twitter', type=str)

    args = parser.parse_args()
    melbGrid = helper.MelbGrid(args.melbgrid)
    


if __name__ == "__main__":
    main()

