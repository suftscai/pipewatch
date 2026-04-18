"""pipewatch CLI — entry point exposed via console_scripts."""

from __future__ import annotations

import argparse
import sys

from pipewatch.cli.runner import run_once, run_watch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Lightweight CLI monitor for ETL pipeline health.",
    )
    parser.add_argument("log_file", help="Path to the pipeline log file.")
    parser.add_argument(
        "--watch",
        action="store_true",
        default=False,
        help="Continuously watch the file for new events (default: snapshot).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        metavar="SECONDS",
        help="Polling interval in watch mode (default: 2.0).",
    )
    parser.add_argument(
        "--tail",
        type=int,
        default=200,
        metavar="N",
        help="Number of recent lines to analyse (default: 200).",
    )
    parser.add_argument(
        "--failure-rate",
        type=float,
        default=0.25,
        metavar="RATE",
        help="Alert threshold for failure rate 0-1 (default: 0.25).",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=10,
        metavar="N",
        help="Alert threshold for absolute failure count (default: 10).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    from pipewatch.analysis.alert import AlertRule

    rules = [
        AlertRule(name="High failure rate", failure_rate_threshold=args.failure_rate),
        AlertRule(name="Too many errors", absolute_failure_threshold=args.max_failures),
    ]

    if args.watch:
        run_watch(args.log_file, interval=args.interval, window=args.tail, rules=rules)
    else:
        print(run_once(args.log_file, tail_n=args.tail, rules=rules))


if __name__ == "__main__":
    main()
