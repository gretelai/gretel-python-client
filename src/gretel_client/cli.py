import sys
import os
import io
import argparse
from collections.abc import Iterable
from typing import IO, Iterator

from gretel_client import get_cloud_client, Client
from gretel_client.readers import CsvReader, JsonReader
from gretel_client.samplers import ConstantSampler


GRETEL_API_KEY = os.getenv('GRETEL_API_KEY')
GRETEL_ENV = os.getenv('GRETEL_ENV', 'api')


class SeekableStreamBuffer(Iterable):
    def __init__(
        self,
        source_stream: IO[bytes],
        encoding: str = 'utf-8',
        line_delimiter: str = '\n'
    ):
        self.source_stream = source_stream
        self.encoding = encoding
        self.line_delimiter = line_delimiter

        self.buffered_source = io.StringIO()

    def seek(self, position: int = 0):
        self.buffered_source.seek(position)

    def __iter__(self) -> Iterator:  # pragma: no cover
        return self

    def __next__(self) -> str:
        line = self.readline().strip()
        if len(line) == 0:
            raise StopIteration()
        return line

    def read(self, size: int = None) -> str:
        read_so_far = self.buffered_source.read(size)
        bytes_to_go = size - len(read_so_far)

        if bytes_to_go > 0:
            from_source = self.source_stream.read(bytes_to_go) \
                .decode(self.encoding)
            self.buffered_source.write(from_source)
            read_so_far += from_source

        return read_so_far

    def readline(self) -> str:
        line = self.buffered_source.readline()
        if not line.endswith(self.line_delimiter):
            from_source = self.source_stream.readline() \
                .decode(self.encoding) \
                .strip()
            self.buffered_source.write(from_source + self.line_delimiter)
            line += from_source
        return line


def parse_command():
    parser = argparse.ArgumentParser(prog='GRETEL')
    # common
    parser.add_argument(
        '-p', '--project', help="Gretel project name", required=True)
    parser.add_argument('--api-key', help="Gretel api key",
                        default=GRETEL_API_KEY)
    subparsers = parser.add_subparsers(help='sub-command help')

    # write
    write_parser = subparsers.add_parser(
        'write', help='write records to gretel project')
    write_parser.add_argument('--reader', default='csv',
                              help='valid readers include: csv and json')
    write_parser.add_argument('--file')
    write_parser.add_argument('--stdin', action='store_true')
    write_parser.add_argument('--sample-rate', default=1, type=int)
    write_parser.add_argument('--max-records', default=-1, type=int)
    write_parser.set_defaults(func=write)

    # tail
    tail_parser = subparsers.add_parser('tail', help='tail project records')
    tail_parser.set_defaults(func=tail)

    return parser


def reader_from_args(args, input_source):
    if args.reader == 'csv':
        return CsvReader(input_source)
    if args.reader == 'json':
        return JsonReader(input_source)
    raise Exception(f"Reader {args.reader} not found.")


def write(args, gretel_client: Client):
    """command handler for `gretel write`"""
    if args.file:
        input_source = args.file
    elif args.stdin:
        # unix pipes aren't seekable. for certain readers we need
        # to be able to seek and replay bytes from a stream.
        input_source = SeekableStreamBuffer(sys.stdin.buffer)
    else:
        raise Exception("No valid input stream passed. Valid inputs include "
                        "--file or --stdin.")

    reader = reader_from_args(args, input_source)
    sampler = ConstantSampler(sample_rate=args.sample_rate,
                              record_limit=args.max_records)

    gretel_client._write_records(project=args.project, sampler=sampler, reader=reader)


def tail(args, gretel_client: Client):
    """command handler for `gretel tail`"""
    if args.project:
        iterator = gretel_client._iter_records(args.project, direction='forward')
        for record in iterator:
            print(record)


def main():
    """`gretel` script entrypoint"""
    parser = parse_command()
    command = parser.parse_args()

    if not GRETEL_API_KEY and not command.api_key:
        raise Exception("Gretel API key not set.")

    gretel_client = get_cloud_client(GRETEL_ENV, command.api_key)

    try:
        command.func(command, gretel_client)
    except AttributeError:
        parser.print_help()
        parser.exit()


if __name__ == '__main__':
    main()
