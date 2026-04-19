"""CLI sub-command: forecast failure rate for a pipeline."""
from __future__ import annotations
import argparse
from pipewatch.export.history import load_history
from pipewatch.export.forecast import compute_forecast, format_forecast


def add_forecast_subparser(subparsers) -> None:
    p = subparsers.add_parser("forecast", help="Forecast failure rate for a pipeline")
    p.add_argument("pipeline", help="Pipeline name to forecast")
    p.add_argument("--history", default="pipewatch_history.jsonl", help="History file path")
    p.add_argument("--steps", type=int, default=3, help="Number of future periods to predict")
    p.add_argument("--window", type=int, default=10, help="History window size")


def run_forecast_cmd(args: argparse.Namespace) -> None:
    history = load_history(args.history)
    report = compute_forecast(
        history,
        pipeline=args.pipeline,
        steps=args.steps,
        window=args.window,
    )
    print(format_forecast(report))
