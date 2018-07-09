#!/usr/bin/env python

from netCDF4 import Dataset
import numpy as np
from argparse import ArgumentParser
from csv import DictWriter
from os import linesep as NEWLINE

METRICS = ["MEAN_ERROR",
             "COEFFICIENT_OF_DETERMINATION",
             "PEARSON_CORRELATION_COEFFICIENT",
             "SAMPLE_SIZE",
             "ROOT_MEAN_SQUARE_ERROR",
             "MEAN_ABSOLUTE_ERROR",
             "INDEX_OF_AGREEMENT",
             "KLING_GUPTA_EFFICIENCY",
             "BIAS_FRACTION",
             "SUM_OF_SQUARE_ERROR",
             "MEAN_SQUARE_ERROR_SKILL_SCORE",
             "MEAN_SQUARE_ERROR",
             "VOLUMETRIC_EFFICIENCY"]
HEADER = ["location", "metric", "value"]


def create_commandline_parser():
    parser = ArgumentParser(description="Prints Metric Information from WRES Netcdf Output")
    parser.add_argument("-g", action="store_true", help="Target file is gridded")
    parser.add_argument("-t", metavar="target", type=str, help="The file to print", required=True)
    parser.add_argument("-o", metavar="output", type=str, help="File to output results to")
    return parser


def get_value_at_index(dataset, parameter_name, index):
    parameter = np.array(dataset.variables[parameter_name])
    return parameter[index]


class NetcdfCSV(object):
    def __init__(self, output_path=None):
        self.__output = None
        self.__writer = None
        self.__contents = None

        if output_path is not None:
            self.__output = open(output_path, 'w')
            self.__writer = DictWriter(self.__output, HEADER)
            self.__writer.writeheader()
        else:
            self.__contents = ",".join(HEADER)

    def add(self, **kwargs):
        if self.__output:
            self.__writer.writerow(kwargs)
        else:
            values = NEWLINE
            values += kwargs["location"] + ","
            values += kwargs["metric"] + ","
            values += kwargs["value"]
            self.__contents += values

    def close(self):
        if self.__output:
            self.__output.close()
        else:
            print self.__contents

def print_contents(path, is_gridded, output):
    printer = NetcdfCSV(output)
    with Dataset(path, 'r') as data:
        for variable_name in METRICS:
            if variable_name not in data.variables:
                continue
            values = dict()
            variable = data.variables[variable_name]
            value_array = np.array(variable)

            if is_gridded:
                for y in range(3840):
                    for x in range(4608):
                        if value_array[y][x] != -999.0:
                            printer.add(location="{},{}".format(y, x), metric=variable_name, value=str(value_array[y][x]))
            else:
                for i in range(len(value_array)):
                    if value_array[i] != -999.0:
                        values[(i)] = value_array[i]
                        printer.add(location="{}".format(get_value_at_index(data, "feature_id", i)), metric=variable_name, value=str(value_array[i]))
    printer.close()


if __name__ == "__main__":
    parser = create_commandline_parser()
    parameters = parser.parse_args()

    print_contents(parameters.t, parameters.g, parameters.o)
