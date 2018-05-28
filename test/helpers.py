import os

import yaml

from xcts.context import ServiceContext


def new_demo_service_context() -> ServiceContext:
    ctx = ServiceContext(base_dir=get_res_dir())
    config_file = os.path.join(ctx.base_dir, 'demo.yml')
    with open(config_file) as fp:
        ctx.config = yaml.load(fp)
    return ctx


def get_res_dir() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'xcts', 'res'))
