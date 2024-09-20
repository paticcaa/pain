#!/bin/python3
import argparse
import subprocess

def run_in_shell(cmd):
    results = subprocess.run(cmd, shell=True, universal_newlines=True, check=True)
    if results.stdout:
        print(results.stdout)

def build_project(args):
    cmd = ['xmake', 'b', '-v'] + args.rest
    run_in_shell(' '.join(cmd))
    pass

def format_project():
    run_in_shell('xmake format -f "**.h:**.cc:**.proto"')
    run_in_shell('git --no-pager diff')

def line_project():
    run_in_shell('find include src -iname "*.h" -o -iname "*.cc" | xargs wc -l')

def main(args):
    if args.subparser_name == 'format':
        format_project()
    elif args.subparser_name == 'build':
        build_project(args)
    elif args.subparser_name == 'line':
        line_project()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('-g', '--global')
    subparsers = parser.add_subparsers(dest="subparser_name", required=True)
    format_parser = subparsers.add_parser('format')
    build_parser = subparsers.add_parser('build')
    build_parser.add_argument('rest', nargs=argparse.REMAINDER)
    line_parser = subparsers.add_parser('line')
    args = parser.parse_args()
    main(args)
