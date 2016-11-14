#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys


def init_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('/tmp/prdshift.log')
    console = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s]-%(levelname)s-%(message)s',
                                  datefmt='%a, %d %b %Y %H:%M:%S')
    fh.setFormatter(formatter)
    console.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(console)
    return logger
