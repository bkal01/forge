import argparse

from forge.commands.submit import handle_submit
from forge.commands.worker import handle_worker
from forge.commands.logs import handle_logs
from forge.commands.list import handle_list


def main() -> None:
    parser = argparse.ArgumentParser(prog="forge")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit_parser = subparsers.add_parser("submit", help="submit a shell script to the job queue")
    submit_parser.add_argument("filename", help="path to file to submit")

    subparsers.add_parser("worker", help="starts forge worker")
    subparsers.add_parser("list", help="list currently running jobs")
    logs_parser = subparsers.add_parser("logs", help="show logs for a job")
    logs_parser.add_argument("job_id", help="job uuid")
    logs_parser.add_argument("-f", "--follow", action="store_true", help="follow log output")

    args = parser.parse_args()

    if args.command == "submit":
        handle_submit(args.filename)
    elif args.command == "worker":
        handle_worker()
    elif args.command == "logs":
        handle_logs(args.job_id, args.follow)
    elif args.command == "list":
        handle_list()
if __name__ == "__main__":
    main()
