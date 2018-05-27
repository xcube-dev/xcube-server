import os

import yaml

from xcts.context import ServiceContext


def new_demo_service_context() -> ServiceContext:
    base_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
    ctx = ServiceContext(base_dir=base_dir)

    config_file = os.path.join(base_dir, 'demo.yml')
    with open(config_file) as fp:
        ctx.config = yaml.load(fp)

    return ctx
