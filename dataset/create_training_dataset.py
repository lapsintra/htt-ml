#!/usr/bin/env python

import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True  # disable ROOT internal argument parser

import argparse
import yaml
import os
import subprocess
from array import array

import logging
logger = logging.getLogger("create_training_dataset")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def parse_arguments():
    logger.debug("Parse arguments.")
    parser = argparse.ArgumentParser(description="Create training dataset")
    parser.add_argument("config", help="Datasets config file")
    return parser.parse_args()


def parse_config(filename):
    logger.debug("Load YAML config: {}".format(filename))
    return yaml.load(open(filename, "r"))


def main(args, config):
    for num_fold in range(2):
        logger.info("Merge input files for fold {}.".format(num_fold))
        created_files = []
        for process in config["processes"]:
            logger.debug("Collect events of process {} for fold {}.".format(
                process, num_fold))

            # Collect all files for this process in chains
            basepaths = []
            basepaths.append(config["base_path"])
            basepaths.extend(config["friend_dirs"])

            created_files.append([])
            for i, basepath in enumerate(basepaths):
                chains = []
                for basepath in basepaths:
                    chain = ROOT.TChain(config["tree_path"])
                    for filename in config["processes"][process]["files"]:
                        path = os.path.join(basepath, filename)
                        if not os.path.exists(path):
                            logger.fatal("File does not exist: {}".format(path))
                        chain.AddFile(path)
                    chains.append(chain)
                # Create output files
                if i == 0: # base chain
                    created_files[-1].append(
                        os.path.join(config["output_path"],
                                     "merge_fold{}_{}.root".format(num_fold, process)))
                else:
                    created_files[-1].append(
                        os.path.join(config["output_path"],
                                     "{}_merge_fold{}_{}.root".format(config["friend_aliases"][i-1], num_fold, process)))

                file_ = ROOT.TFile(created_files[-1][-1], "RECREATE")

                chain_numentries = chains[0].GetEntries()
                if not chain_numentries > 0:
                    logger.fatal(
                        "Chain (before skimming) does not contain any events.")
                    raise Exception
                logger.debug("Found {} events for process {}.".format(
                    chain_numentries, process))

                for j, friend in enumerate(chains):
                    if i != j:
                        alias = config["friend_aliases"][j-1] if j else ""
                        chains[i].AddFriend(friend, alias)

                # Skim the events with the cut string
                cut_string = "({EVENT_BRANCH}%2=={NUM_FOLD})&&({CUT_STRING})".format(
                    EVENT_BRANCH=config["event_branch"],
                    NUM_FOLD=num_fold,
                    CUT_STRING=config["processes"][process]["cut_string"])
                if i:
                    pattern = "{}.".format(config["friend_aliases"][i-1])
                    cut_string = cut_string.replace(pattern, "")
                logger.debug("Skim events with cut string: {}".format(cut_string))

                chain_skimmed = chains[i].CopyTree(cut_string)
                chain_skimmed_numentries = chain_skimmed.GetEntries()
                if not chain_skimmed_numentries > 0:
                    logger.fatal(
                        "Chain (after skimming) does not contain any events.")
                    raise Exception
                logger.debug("Found {} events for process {} after skimming.".
                             format(chain_skimmed_numentries, process))

                flist = chains[i].GetListOfFriends()
                if flist:
                    flist.Clear()
                for chain in chains:
                    chain.Reset()

                if i == 0: # base chain
                    # Write training weight to new branch
                    logger.debug("Add training weights with weight string: {}".format(
                        config["processes"][process]["weight_string"]))
                    formula = ROOT.TTreeFormula(
                        "training_weight",
                        config["processes"][process]["weight_string"], chain_skimmed)
                    training_weight = array('f', [-999.0])
                    branch_training_weight = chain_skimmed.Branch(
                        config["training_weight_branch"], training_weight,
                        config["training_weight_branch"] + "/F")
                    for i_event in range(chain_skimmed.GetEntries()):
                        chain_skimmed.GetEntry(i_event)
                        training_weight[0] = formula.EvalInstance()
                        branch_training_weight.Fill()

                # Rename chain to process name and write to output file
                logger.debug("Write output file for this process and fold.")
                chain_skimmed.SetName(config["processes"][process]["class"])
                chain_skimmed.Write()
                file_.Close()


        # Combine all skimmed files using `hadd`
        logger.debug("Call `hadd` to combine files of processes for fold {}.".
                     format(num_fold))
        for i, chain in enumerate(chains):
            if i == 0:
                output_file = os.path.join(config["output_path"], "fold{}_{}".format(
                    num_fold, config["output_filename"]))
            else:
                output_file = os.path.join(config["output_path"], "{}_fold{}_{}".format(
                    config["friend_aliases"][i-1], num_fold, config["output_filename"]))
            sub_created_files = [process_files[i] for process_files in created_files]
            print(["hadd", "-f", output_file] + sub_created_files)
            subprocess.call(["hadd", "-f", output_file] + sub_created_files)
            logger.info("Created output file: {}".format(output_file))


if __name__ == "__main__":
    args = parse_arguments()
    config = parse_config(args.config)
    main(args, config)
