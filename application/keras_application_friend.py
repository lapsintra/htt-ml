#!/usr/bin/env python

import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True  # disable ROOT internal argument parser
ROOT.gErrorIgnoreLevel = ROOT.kError

import argparse
from array import array
import yaml
import pickle
import numpy as np
import os

from keras.models import load_model


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Apply machine learning methods for Htt analyses")
    parser.add_argument("config_dataset", help="Path to dataset config file")
    parser.add_argument("config_training", help="Path to training config file")
    parser.add_argument(
        "config_application", help="Path to application config file")
    parser.add_argument(
        "input", help="Path to input file, where response will be added.")
    parser.add_argument("tree", help="Path to tree in the ROOT input file")
    return parser.parse_args()


def parse_config(filename):
    return yaml.load(open(filename, "r"))


def main(args, config_dataset, config_training, config_application):
    # Load keras model and preprocessing
    classifiers = []
    preprocessing = []
    for c, p in zip(config_application["classifiers"],
                    config_application["preprocessing"]):
        classifiers.append(load_model(c))
        preprocessing.append(pickle.load(open(p, "rb")))

    # Open input file and register branches with input variables
    file_input = ROOT.TFile(args.input)
    if file_input == None:
        raise Exception("File is not existent: {}".format(args.input))

    tree_input = file_input.Get(args.tree)
    if tree_input == None:
        raise Exception("Input tree {} is not existent in file: {}".format(
            args.tree, args.input))

    friend_inputs = []
    for fdir in config_dataset["friend_dirs"]:
        basename = "/".join(args.input.split("/")[-2:])
        path = os.path.join(fdir, basename)
        friend_input = ROOT.TFile(path, "READ")
        if friend_input == None:
            raise Exception("File is not existent: {}".format(path))
        friend_inputs.append(friend_input)

    ftrees = []
    for i, finput in enumerate(friend_inputs):
        ftree = finput.Get(args.tree)
        if ftree == None:
            raise Exception("Input tree {} is not existent in file: {}".format(
                args.tree, repr(finput)))
        tree_input.AddFriend(ftree, config_dataset["friend_aliases"][i])
        ftrees.append(ftree)

    values = []
    for variable in config_training["variables"]:
        typename = tree_input.GetLeaf(variable).GetTypeName()
        if  typename == "Float_t":
            values.append(array("f", [-999]))
        elif typename == "Int_t":
            values.append(array("i", [-999]))
        else:
            logger.fatal("Variable {} has unknown type {}.".format(variable, typename))
            raise Exception
        tree_input.SetBranchAddress(variable, values[-1])

    # Open output file and register branches with output variables
    basename = os.path.basename(os.path.dirname(args.input))
    output_directory = os.path.join(config_application["output_directory"],
                                    basename)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    output_filename = os.path.join(config_application["output_directory"],
                                   basename, "{}.root".format(basename))
    if os.path.exists(output_filename):
        file_output = ROOT.TFile(output_filename, "UPDATE")
    else:
        file_output = ROOT.TFile(output_filename, "RECREATE")
    if file_output == None:
        raise Exception("File can not be updated: {}".format(output_filename))

    tree_dir = os.path.dirname(args.tree)
    if file_output.mkdir(tree_dir) == None:
        raise Exception("Directory {} did already exist for file {}.".format(
            tree_dir, args.input))
    file_output.cd(tree_dir)
    tree_output = ROOT.TTree(
        os.path.basename(args.tree), os.path.dirname(args.tree))

    response_branches = []
    response_single_scores = []
    prefix = config_application["branch_prefix"]
    for class_ in config_training["classes"]:
        response_single_scores.append(array("f", [-999]))
        response_branches.append(
            tree_output.Branch("{}{}".format(prefix, class_),
                               response_single_scores[-1], "{}{}/F".format(
                                   prefix, class_)))

    response_max_score = array("f", [-999])
    response_branches.append(
        tree_output.Branch("{}max_score".format(prefix), response_max_score,
                           "{}max_score/F".format(prefix)))

    response_max_index = array("f", [-999])
    response_branches.append(
        tree_output.Branch("{}max_index".format(prefix), response_max_index,
                           "{}max_index/F".format(prefix)))

    # Loop over events and add method's response to tree
    for i_event in range(tree_input.GetEntries()):
        # Get current event
        tree_input.GetEntry(i_event)

        # Get event number and calculate method's response
        event = int(getattr(tree_input, config_dataset["event_branch"]))
        values_stacked = np.hstack(values).reshape(1, len(values))
        values_preprocessed = preprocessing[event %
                                            2].transform(values_stacked)
        response = classifiers[event % 2].predict(values_preprocessed)
        response = np.squeeze(response)

        # Find max score and index
        response_max_score[0] = -999.0
        for i, r in enumerate(response):
            response_single_scores[i][0] = r
            if r > response_max_score[0]:
                response_max_score[0] = r
                response_max_index[0] = i

        # Fill branches
        tree_output.Fill()

    # Write new branches to output file
    file_input.Close()
    for file_ in friend_inputs:
        file_.Close()
    file_output.Write()
    file_output.Close()


if __name__ == "__main__":
    args = parse_arguments()
    config_dataset = parse_config(args.config_dataset)
    config_training = parse_config(args.config_training)
    config_application = parse_config(args.config_application)
    main(args, config_dataset, config_training, config_application)
