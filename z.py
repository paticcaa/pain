#!/bin/python3
import argparse
import subprocess
import logging
import os
import json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("pain") 


def run_in_shell(cmd, stdout=None, stderr=None):
    results = subprocess.run(cmd, shell=True, universal_newlines=True, check=True, stdout=stdout, stderr=stderr)
    if results.returncode != 0:
        raise Exception("Failed to run command: {}".format(cmd))
    if results.stdout:
        print(results.stdout)
    if results.stderr:
        print(results.stderr)

def config_project(args):
    print('Unimplemented')


def get_modules():
    modules = {
        'deva': '//src/deva:deva',
        'asura': '//src/asura:asura',
        'manusya': '//src/manusya:manusya',
        'pain': '//src/pain:pain',
        'sad': '//src/sad:sad',
        'test_pain_base': '//src/base:test_pain_base',
        'test_deva': '//src/deva:test_deva',
        'test_asura': '//src/asura:test_asura',
        'test_manusya': '//src/manusya:test_manusya',
    }
    return modules

def get_targets(names):
    modules = get_modules()
    targets = []
    if not names:
        targets = ['//src/...']
    else:
        for target in names:
            if target in modules:
                targets.append(modules[target])
            else:
                print('Unknown target: {}'.format(target))

    return targets

def show_targets(args):
    modules = get_modules()
    print(json.dumps(modules, indent=4))

def build_project(args):
    targets = get_targets(args.rest)

    cmd = ['bazel', 'build', '-s', '--verbose_failures', '--disk_cache={}'.format(args.cache_dir)] + targets
    print('> ' + ' '.join(cmd))
    run_in_shell(' '.join(cmd))
    if args.install:
        print('Unimplemented')

def test_project(args):
    targets = get_targets(args.rest)
    cmd = [
        'bazel',
        'test',
        '-s',
        '--verbose_failures',
        '--test_output=errors',
        '--cache_test_results=no',
        '--disk_cache={}'.format(args.cache_dir)
    ] + targets
    print('> ' + ' '.join(cmd))
    run_in_shell(' '.join(cmd))

def clang_format(path):
    run_in_shell(r"find {} -regex '.*\.\(cc\|h\|proto\)' | xargs -n1 -P $(nproc) clang-format -i --style=file --fallback-style=none".format(path))


def lint(dir, include_files, exclude_files):
        filename = os.path.basename(dir)
        nproc = os.cpu_count()
        logger.info(f"check {filename} with include_files: {include_files} and exclude_files: {exclude_files}")
        # find src -type f \( -name "*.cc" -o -name "*.h" \) ! \( -name "demo.cc" -o -name "hello.cc" \)
        include_files_str = ' -o '.join(['-name "{}"'.format(file) for file in include_files])
        exclude_files_str = ' -o '.join(['-path "{}"'.format(file) for file in exclude_files])
        find_cmd = r'find {} -type f \( {} \) ! \( {} \)'.format(dir, include_files_str, exclude_files_str)
        cmd = '{} | xargs -n1 -P {} clang-tidy --use-color 2>/dev/null | tee clang-tidy-{}.log'.format(find_cmd, nproc, filename)
        run_in_shell(cmd, None, None)
        # grep -cP "readability|modernize" clang-tidy.log
        error_lines = []
        error_count = 0
        check_items = ["readability", "modernize", "cppcoreguidelines"]
        with open("clang-tidy-{}.log".format(filename), "r") as f:
            for line in f:
                for item in check_items:
                    if item in line:
                        error_count += 1
                        error_lines.append(line.strip())
                        break
        return {"error_count": error_count, "error_lines": error_lines}

def lint_project(args):
    include_files = [
        '*.cc',
        '*.h',
    ]
    exclude_files = [
        '*examples*',
    ]
    check_dirs = [
        'src',
        'include',
    ]
    errors = {}
    for dir in check_dirs:
        errors[dir] = lint(dir, include_files, exclude_files)
    
    error_count = 0
    for filename, error_info in errors.items():
        if error_info["error_count"] > 0:
            error_count += error_info["error_count"]
            logger.info(f"Found {error_info['error_count']} lint errors in {filename}")
            for line in error_info["error_lines"]:
                logger.info(line)
        else:
            logger.info(f"No lint error found in {filename}")
    if error_count > 0:
        raise Exception("\033[31mFound {} lint errors\033[0m".format(error_count))
    else:
        logger.info("\033[32mCheck lint error success\033[0m")



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
    run_in_shell('bazel run --disk_cache={} //:install'.format(args.cache_dir))


def deploy_project(args):
    if args.action == 'start':
        run_in_shell('ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start')
    elif args.action == 'stop':
        run_in_shell('ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t stop')
    elif args.action == 'restart':
        run_in_shell('ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t stop')
        run_in_shell('ansible-playbook -i ./deploy/hosts ./deploy/deploy.yml -t start')
    else:
        print('Unimplemented')

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
    build_parser.add_argument('--cache-dir', default=os.getenv("PAIN_BAZEL_CACHE_DIR", '/mnt/bazel_cache'))
    build_parser.add_argument('rest', nargs=argparse.REMAINDER)

    line_parser = subparsers.add_parser('line')
    line_parser.set_defaults(func=line_project)

    install_parser = subparsers.add_parser('install', aliases=['i'])
    install_parser.set_defaults(func=install_project)
    install_parser.add_argument('--cache-dir', default=os.getenv("PAIN_BAZEL_CACHE_DIR", '/mnt/bazel_cache'))
    install_parser.add_argument('rest', nargs=argparse.REMAINDER)

    deploy_parser = subparsers.add_parser('deploy', aliases=['d'])
    deploy_parser.set_defaults(func=deploy_project)
    deploy_parser.add_argument('-a', '--action', choices=['start', 'stop', 'restart'], required=True)
    deploy_parser.add_argument('rest', nargs=argparse.REMAINDER)

    lint_parser = subparsers.add_parser('lint', aliases=['l'])
    lint_parser.set_defaults(func=lint_project)
    lint_parser.add_argument('rest', nargs=argparse.REMAINDER)

    test_parser = subparsers.add_parser('test', aliases = ['t'])
    test_parser.add_argument('--cache-dir', default=os.getenv("PAIN_BAZEL_CACHE_DIR", '/mnt/bazel_cache'))
    test_parser.set_defaults(func=test_project)
    test_parser.add_argument('rest', nargs=argparse.REMAINDER)

    show_parser = subparsers.add_parser('show', aliases=['s'])
    show_parser.set_defaults(func=show_targets)

    args = parser.parse_args()
    args.func(args)
