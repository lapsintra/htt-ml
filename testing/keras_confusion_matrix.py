#!/usr/bin/env python

import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True  # disable ROOT internal argument parser

import argparse
from array import array
import yaml
import pickle
import numpy as np
import os

import matplotlib as mpl
mpl.use('Agg')
mpl.rcParams['font.size'] = 16
import matplotlib.pyplot as plt
from matplotlib import cm

from keras.models import load_model

import logging
logger = logging.getLogger("keras_confusion_matrix")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Produce confusion matrice")
    parser.add_argument("config_training", help="Path to training config file")
    parser.add_argument("config_testing", help="Path to testing config file")
    parser.add_argument("fold", type=int, help="Trained model to be tested.")
    parser.add_argument(
        "--friends-config", required=True, help="Config for custom friends"
    )
    return parser.parse_args()


def parse_config(filename):
    logger.debug("Load config %s.", filename)
    return yaml.load(open(filename, "r"))


def get_efficiency_representations(m):
    ma = np.zeros(m.shape)
    mb = np.zeros(m.shape)
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            ma[i, j] = m[i, j] / m[i, i]
            mb[i, j] = m[i, j] / np.sum(m[i, :])
    return ma, mb


def get_purity_representations(m):
    ma = np.zeros(m.shape)
    mb = np.zeros(m.shape)
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            ma[i, j] = m[i, j] / m[j, j]
            mb[i, j] = m[i, j] / np.sum(m[:, j])
    return ma, mb


def plot_confusion(confusion, classes, filename, markup='{:.2f}'):
    logger.debug("Write plot to %s.", filename)
    plt.figure(figsize=(2.5 * confusion.shape[0], 2.0 * confusion.shape[1]))
    axis = plt.gca()
    for i in range(confusion.shape[0]):
        for j in range(confusion.shape[1]):
            axis.text(
                i + 0.5,
                j + 0.5,
                markup.format(confusion[-1 - j, i]),
                ha='center',
                va='center')
    q = plt.pcolormesh(confusion[::-1], cmap='Wistia')
    cbar = plt.colorbar(q)
    cbar.set_label("Sum of event weights", rotation=270, labelpad=50)
    plt.xticks(
        np.array(range(len(classes))) + 0.5, classes, rotation='vertical')
    plt.yticks(
        np.array(range(len(classes))) + 0.5,
        classes[::-1],
        rotation='horizontal')
    plt.xlim(0, len(classes))
    plt.ylim(0, len(classes))
    plt.ylabel('True class')
    plt.xlabel('Predicted class')
    plt.savefig(filename, bbox_inches='tight')


def print_matrix(p, title):
    stdout.write(title + '\n')
    for i in range(p.shape[0]):
        stdout.write('    ')
        for j in range(p.shape[1]):
            stdout.write('{:.4f} & '.format(p[i, j]))
        stdout.write('\b\b\\\\\n')


def main(args, config_test, config_train, config_friends):
    path = os.path.join(config_train["output_path"],
                        config_test["preprocessing"][args.fold])
    logger.info("Load preprocessing %s.", path)
    preprocessing = pickle.load(open(path, "rb"))

    path = os.path.join(config_train["output_path"],
                        config_test["model"][args.fold])
    logger.info("Load keras model %s.", path)
    model = load_model(path)

    path = os.path.join(config_train["datasets"][(1, 0)[args.fold]])
    logger.info("Loop over test dataset %s to get model response.", path)
    file_ = ROOT.TFile(path)
    ffiles = []
    for falias in config_friends["friend_aliases"]:
        basename = os.path.basename(path)
        dirname = os.path.dirname(path)
        ffile = ROOT.TFile("{}/{}_{}".format(dirname, falias, basename), "READ")
        ffiles.append(ffile)
    confusion = np.zeros(
        (len(config_train["classes"]), len(config_train["classes"])),
        dtype=np.float)
    for i_class, class_ in enumerate(config_train["classes"]):
        logger.debug("Process class %s.", class_)

        tree = file_.Get(class_)
        if tree == None:
            logger.fatal("Tree %s does not exist.", class_)
            raise Exception

        ftrees = []
        for i_ffile, ffile in enumerate(ffiles):
            ftree = ffile.Get(class_)
            if ftree == None:
                logger.fatal("Tree %s does not exist in %s.", class_, repr(ffile))
                raise Exception
            tree.AddFriend(ftree, config_friends["friend_aliases"][i_ffile])
            ftrees.append(ftree)

        values = []
        for variable in config_train["variables"]:
            typename = tree.GetLeaf(variable).GetTypeName()
            if  typename == "Float_t":
                values.append(array("f", [-999]))
            elif typename == "Int_t":
                values.append(array("i", [-999]))
            else:
                logger.fatal("Variable {} has unknown type {}.".format(variable, typename))
                raise Exception
            tree.SetBranchAddress(variable, values[-1])

        if tree.GetLeaf(variable).GetTypeName() != "Float_t":
            logger.fatal("Weight branch has unkown type.")
            raise Exception
        weight = array("f", [-999])
        tree.SetBranchAddress(config_test["weight_branch"], weight)

        for i_event in range(tree.GetEntries()):
            tree.GetEntry(i_event)
            values_stacked = np.hstack(values).reshape(1, len(values))
            values_preprocessed = preprocessing.transform(values_stacked)
            response = model.predict(values_preprocessed)
            response = np.squeeze(response)
            max_index = np.argmax(response)
            confusion[i_class, max_index] += weight[0]

    logger.info("Write confusion matrices.")

    # Standard confusion matrix
    path_template = os.path.join(config_train["output_path"],
                                 "fold{}_keras_confusion_{}.png")

    plot_confusion(confusion, config_train["classes"],
                   path_template.format(args.fold, "standard"))

    confusion_eff1, confusion_eff2 = get_efficiency_representations(confusion)
    plot_confusion(confusion_eff1, config_train["classes"],
                   path_template.format(args.fold, "efficiency1"))
    plot_confusion(confusion_eff2, config_train["classes"],
                   path_template.format(args.fold, "efficiency2"))

    confusion_pur1, confusion_pur2 = get_purity_representations(confusion)
    plot_confusion(confusion_pur1, config_train["classes"],
                   path_template.format(args.fold, "purity1"))
    plot_confusion(confusion_pur2, config_train["classes"],
                   path_template.format(args.fold, "purity2"))


if __name__ == "__main__":
    args = parse_arguments()
    config_test = parse_config(args.config_testing)
    config_train = parse_config(args.config_training)
    config_friends = parse_config(args.friends_config)
    main(args, config_test, config_train, config_friends)
