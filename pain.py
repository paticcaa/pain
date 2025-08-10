#!/bin/python3
import argparse
import subprocess

def run_in_shell(cmd):
    results = subprocess.run(cmd, shell=True, universal_newlines=True, check=True)
    if results.stdout:
        print(results.stdout)
    if results.stderr:
        print(results.stderr)
    if results.returncode != 0:
        raise Exception("Failed to run command: {}".format(cmd))

def config_project(args):
    if args.clean:
        run_in_shell('xmake f -c')
        return
    run_in_shell('xmake f -cv --mode={}'.format(args.mode))

def build_project(args):
    cmd = ['xmake', 'b', '-v'] + args.rest
    run_in_shell(' '.join(cmd))
    if args.install:
        run_in_shell('xmake install -o output')
    pass

def clang_format(path):
    run_in_shell(r"find {} -regex '.*\.\(cc\|h\|proto\)' | xargs -n1 -P $(nproc) clang-format -i --style=file --fallback-style=none".format(path))

def format_project(args):
    paths = [
        'include',
        'src',
        'protocols',
    ]
    for path in paths:
        clang_format(path)

def line_project(args):
    run_in_shell('find include src -iname "*.h" -o -iname "*.cc" -o -iname "*.c" | xargs wc -l')

def install_project(args):
    if not args.rest:
        run_in_shell('xmake install -o output')
        return
    for target in args.rest:
        run_in_shell('xmake install -o output {}'.format(target))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('-g', '--global')
    subparsers = parser.add_subparsers(dest="subparser_name", required=True)
    format_parser = subparsers.add_parser('format', aliases=['f'])
    format_parser.set_defaults(func=format_project)

    config_parser = subparsers.add_parser('config')
    config_parser.set_defaults(func=config_project)
    config_parser.add_argument('-c', '--clean', action='store_true')
    config_parser.add_argument('-m', '--mode', default='debug', choices=['debug', 'releasedbg'])

    build_parser = subparsers.add_parser('build', aliases=['b'])
    build_parser.set_defaults(func=build_project)
    build_parser.add_argument('-i', '--install', action='store_true')
    build_parser.add_argument('rest', nargs=argparse.REMAINDER)

    line_parser = subparsers.add_parser('line')
    line_parser.set_defaults(func=line_project)

    install_parser = subparsers.add_parser('install')
    install_parser.set_defaults(func=install_project)
    install_parser.add_argument('rest', nargs=argparse.REMAINDER)

    args = parser.parse_args()
    args.func(args)
