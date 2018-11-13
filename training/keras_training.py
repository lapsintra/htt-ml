#!/usr/bin/env python

import logging
logger = logging.getLogger("keras_training")

import argparse
import yaml
import os
import pickle


def parse_arguments():
    logger.debug("Parse arguments.")
    parser = argparse.ArgumentParser(
        description="Train machine Keras models for Htt analyses")
    parser.add_argument("config", help="Path to training config file")
    parser.add_argument("fold", type=int, help="Select the fold to be trained")
    parser.add_argument(
        "--friends-config", required=True, help="Config for custom friends"
    )
    return parser.parse_args()


def parse_config(filename):
    logger.debug("Parse config.")
    return yaml.load(open(filename, "r"))


def setup_logging(level, output_file=None):
    logger.setLevel(level)
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if not output_file == None:
        file_handler = logging.FileHandler(output_file, "w")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


def main(args, config, friends_config):
    # Set seed and import packages
    # NOTE: This need to be done before any keras module is imported!
    logger.debug("Import packages and set random seed to %s.",
                 int(config["seed"]))
    import numpy as np
    np.random.seed(int(config["seed"]))

    import ROOT
    ROOT.PyConfig.IgnoreCommandLineOptions = True  # disable ROOT internal argument parser
    import root_numpy

    from sklearn import preprocessing, model_selection
    import keras_models
    from keras.callbacks import ReduceLROnPlateau, EarlyStopping, ModelCheckpoint

    # Extract list of variables
    variables = config["variables"]
    logger.debug("Use variables:")
    for v in variables:
        logger.debug("%s", v)

    # Load training dataset
    filename = config["datasets"][args.fold]
    logger.debug("Load training dataset from %s.", filename)
    x = []
    y = []
    w = []
    rfile = ROOT.TFile(filename, "READ")
    ffiles = []
    for falias in friends_config["friend_aliases"]:
        basename = os.path.basename(filename)
        dirname = os.path.dirname(filename)
        ffile = ROOT.TFile("{}/{}_{}".format(dirname, falias, basename), "READ")
        ffiles.append(ffile)
    classes = config["classes"]
    for i_class, class_ in enumerate(classes):
        logger.debug("Process class %s.", class_)
        tree = rfile.Get(class_)
        if tree == None:
            logger.fatal("Tree %s not found in file %s.", class_, filename)
            raise Exception

        ftrees = []
        for i_ffile, ffile in enumerate(ffiles):
            ftree = ffile.Get(class_)
            if ftree == None:
                logger.fatal("Tree %s not found in file %s.", class_, repr(ffile))
                raise Exception
            tree.AddFriend(ftree, friends_config["friend_aliases"][i_ffile])
            ftrees.append(ftree)

        # Get inputs for this class
        x_class = np.zeros((tree.GetEntries(), len(variables)))
        x_conv = root_numpy.tree2array(tree, branches=variables)
        for i_var, var in enumerate(variables):
            x_class[:, i_var] = x_conv[var]
        x.append(x_class)

        # Get weights
        w_class = np.zeros((tree.GetEntries(), 1))
        w_conv = root_numpy.tree2array(
            tree, branches=[config["event_weights"]])
        w_class[:,
                0] = w_conv[config["event_weights"]] * config["class_weights"][class_]
        w.append(w_class)

        # Get targets for this class
        y_class = np.zeros((tree.GetEntries(), len(classes)))
        y_class[:, i_class] = np.ones((tree.GetEntries()))
        y.append(y_class)

    # Stack inputs, targets and weights to a Keras-readable dataset
    x = np.vstack(x)  # inputs
    y = np.vstack(y)  # targets
    w = np.vstack(w) * config["global_weight_scale"]  # weights
    w = np.squeeze(w)  # needed to get weights into keras

    # Perform input variable transformation and pickle scaler object
    logger.info("Use preprocessing method %s.", config["preprocessing"])
    if "standard_scaler" in config["preprocessing"]:
        scaler = preprocessing.StandardScaler().fit(x)
        for var, mean, std in zip(variables, scaler.mean_, scaler.scale_):
            logger.debug("Preprocessing (variable, mean, std): %s, %s, %s",
                         var, mean, std)
    elif "identity" in config["preprocessing"]:
        scaler = preprocessing.StandardScaler().fit(x)
        for i in range(len(scaler.mean_)):
            scaler.mean_[i] = 0.0
            scaler.scale_[i] = 1.0
        for var, mean, std in zip(variables, scaler.mean_, scaler.scale_):
            logger.debug("Preprocessing (variable, mean, std): %s, %s, %s",
                         var, mean, std)
    elif "robust_scaler" in config["preprocessing"]:
        scaler = preprocessing.RobustScaler().fit(x)
        for var, mean, std in zip(variables, scaler.center_, scaler.scale_):
            logger.debug("Preprocessing (variable, mean, std): %s, %s, %s",
                         var, mean, std)
    elif "min_max_scaler" in config["preprocessing"]:
        scaler = preprocessing.MinMaxScaler(feature_range=(-1.0, 1.0)).fit(x)
        for var, min_, max_ in zip(variables, scaler.data_min_,
                                   scaler.data_max_):
            logger.debug("Preprocessing (variable, min, max): %s, %s, %s", var,
                         min_, max_)
    elif "quantile_transformer" in config["preprocessing"]:
        scaler = preprocessing.QuantileTransformer(
            output_distribution="normal",
            random_state=int(config["seed"])).fit(x)
    else:
        logger.fatal("Preprocessing %s is not implemented.",
                     config["preprocessing"])
        raise Exception
    x = scaler.transform(x)

    path_preprocessing = os.path.join(
        config["output_path"],
        "fold{}_keras_preprocessing.pickle".format(args.fold))
    logger.info("Write preprocessing object to %s.", path_preprocessing)
    pickle.dump(scaler, open(path_preprocessing, 'wb'))

    # Split data in training and testing
    x_train, x_test, y_train, y_test, w_train, w_test = model_selection.train_test_split(
        x,
        y,
        w,
        test_size=1.0 - config["train_test_split"],
        random_state=int(config["seed"]))

    # Add callbacks
    callbacks = []
    if "early_stopping" in config["model"]:
        logger.info("Stop early after %s tries.",
                    config["model"]["early_stopping"])
        callbacks.append(
            EarlyStopping(patience=config["model"]["early_stopping"]))

    path_model = os.path.join(config["output_path"],
                              "fold{}_keras_model.h5".format(args.fold))
    if "save_best_only" in config["model"]:
        if config["model"]["save_best_only"]:
            logger.info("Write best model to %s.", path_model)
            callbacks.append(
                ModelCheckpoint(path_model, save_best_only=True, verbose=1))

    if "reduce_lr_on_plateau" in config["model"]:
        logger.info("Reduce learning-rate after %s tries.",
                    config["model"]["reduce_lr_on_plateau"])
        callbacks.append(
            ReduceLROnPlateau(
                patience=config["model"]["reduce_lr_on_plateau"], verbose=1))

    # Train model
    if not hasattr(keras_models, config["model"]["name"]):
        logger.fatal("Model %s is not implemented.", config["model"]["name"])
        raise Exception
    logger.info("Train keras model %s.", config["model"]["name"])

    if config["model"]["batch_size"] < 0:
        batch_size = x_train.shape[0]
    else:
        batch_size = config["model"]["batch_size"]

    model_impl = getattr(keras_models, config["model"]["name"])
    model = model_impl(len(variables), len(classes))
    model.summary()
    history = model.fit(
        x_train,
        y_train,
        sample_weight=w_train,
        validation_data=(x_test, y_test, w_test),
        batch_size=batch_size,
        nb_epoch=config["model"]["epochs"],
        shuffle=True,
        callbacks=callbacks)

    # Plot loss
    # NOTE: Matplotlib needs to be imported after Keras/TensorFlow because of conflicting libraries
    import matplotlib as mpl
    mpl.use('Agg')
    import matplotlib.pyplot as plt

    epochs = range(1, len(history.history["loss"]) + 1)
    plt.plot(epochs, history.history["loss"], lw=3, label="Training loss")
    plt.plot(
        epochs, history.history["val_loss"], lw=3, label="Validation loss")
    plt.xlabel("Epoch"), plt.ylabel("Loss")
    path_plot = os.path.join(config["output_path"],
                             "fold{}_loss.png".format(args.fold))
    plt.legend()
    plt.savefig(path_plot, bbox_inches="tight")

    # Save model
    if not "save_best_only" in config["model"]:
        logger.info("Write model to %s.", path_model)
        model.save(path_model)


if __name__ == "__main__":
    setup_logging(logging.DEBUG)
    args = parse_arguments()
    config = parse_config(args.config)
    friends_config = parse_config(args.friends_config)
    main(args, config)
