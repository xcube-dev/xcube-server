import os

import yaml

from xcts.context import ServiceContext


def new_test_service_context() -> ServiceContext:
    ctx = ServiceContext(base_dir=get_res_test_dir())
    config_file = os.path.join(ctx.base_dir, 'config.yml')
    with open(config_file) as fp:
        ctx.config = yaml.load(fp)
    return ctx


def new_demo_service_context() -> ServiceContext:
    ctx = ServiceContext(base_dir=get_res_demo_dir())
    config_file = os.path.join(ctx.base_dir, 'config.yml')
    with open(config_file) as fp:
        ctx.config = yaml.load(fp)
    return ctx


def get_res_test_dir() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), 'res', 'test'))


def get_res_demo_dir() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'xcts', 'res', 'demo'))
