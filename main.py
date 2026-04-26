"""Backwards-compatible entrypoint — delegates to `cli.py benchmark`."""

import sys

from cli import build_parser


def main() -> None:
    sys.argv = [sys.argv[0], "benchmark"] + sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
