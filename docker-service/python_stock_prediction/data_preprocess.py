import numpy as np


def data_preprocess(X, subsample=100):
    N, T, D = X.shape
    X_mean = np.mean(X.swapaxes(1, 2).reshape(N, D, -1, subsample), axis=3).swapaxes(1, 2)
    X_max = np.max(X.swapaxes(1, 2).reshape(N, D, -1, subsample), axis=3).swapaxes(1, 2)
    X_min = np.min(X.swapaxes(1, 2).reshape(N, D, -1, subsample), axis=3).swapaxes(1, 2)
    X_std = np.std(X.swapaxes(1, 2).reshape(N, D, -1, subsample), axis=3).swapaxes(1, 2)

    X_prep = np.concatenate([X_mean, X_max, X_min, X_std], axis=2)
    return X_prep