import argparse

from forge.commands.submit import handle_submit
from forge.commands.worker import handle_worker


def main() -> None:
    parser = argparse.ArgumentParser(prog="forge")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit_parser = subparsers.add_parser("submit", help="submit a shell script to the job queue")
    submit_parser.add_argument("filename", help="path to file to submit")

    subparsers.add_parser("worker", help="starts forge worker")

    args = parser.parse_args()

    if args.command == "submit":
        handle_submit(args.filename)
    elif args.command == "worker":
        handle_worker()
if __name__ == "__main__":
    main()
