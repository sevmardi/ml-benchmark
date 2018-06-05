# Copyright 2018 Criteo
# Licensed under the terms of the Apache 2.0 license.
# Please see LICENSE file in the project root for terms.
# Distributed Criteo Display CTR prediction on grid based on TensorFlow on Spark
# https://github.com/yahoo/TensorFlowOnSpark

from __future__ import absolute_import
from __future__ import division
from __future__ import nested_scopes
from __future__ import print_function


validation_file = None


def print_log(worker_num, arg):
	print("{0}: {1}".format(worker_num, arg))

