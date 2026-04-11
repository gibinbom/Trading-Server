from __future__ import annotations

import numpy as np


class RidgeRegressor:
    def __init__(self, alpha: float = 1.0):
        self.alpha = float(alpha)
        self.beta: np.ndarray | None = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "RidgeRegressor":
        X_ = np.c_[np.ones(len(X)), X]
        X_ = np.nan_to_num(X_, nan=0.0, posinf=0.0, neginf=0.0)
        y = np.nan_to_num(y, nan=0.0, posinf=0.0, neginf=0.0)
        eye = np.eye(X_.shape[1])
        eye[0, 0] = 0.0
        try:
            self.beta = np.linalg.pinv(X_.T @ X_ + self.alpha * eye) @ X_.T @ y
        except Exception:
            self.beta = np.linalg.lstsq(X_.T @ X_ + self.alpha * eye, X_.T @ y, rcond=None)[0]
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self.beta is None:
            return np.zeros(len(X), dtype=float)
        X_ = np.c_[np.ones(len(X)), X]
        X_ = np.nan_to_num(X_, nan=0.0, posinf=0.0, neginf=0.0)
        return X_ @ self.beta


class NumpyMLPRegressor:
    def __init__(self, hidden_dim: int = 16, lr: float = 0.01, epochs: int = 200, l2: float = 1e-4, seed: int = 7):
        self.hidden_dim = int(hidden_dim)
        self.lr = float(lr)
        self.epochs = int(epochs)
        self.l2 = float(l2)
        self.seed = int(seed)
        self.params: dict[str, np.ndarray] = {}

    def fit(self, X: np.ndarray, y: np.ndarray) -> "NumpyMLPRegressor":
        rng = np.random.default_rng(self.seed)
        n, d = X.shape
        h = max(4, min(self.hidden_dim, max(4, d * 2)))
        self.params = {
            "W1": rng.normal(0, 0.15, size=(d, h)),
            "b1": np.zeros((1, h)),
            "W2": rng.normal(0, 0.15, size=(h, 1)),
            "b2": np.zeros((1, 1)),
        }
        y_ = y.reshape(-1, 1)
        for _ in range(self.epochs):
            z1 = X @ self.params["W1"] + self.params["b1"]
            a1 = np.tanh(z1)
            pred = a1 @ self.params["W2"] + self.params["b2"]
            err = pred - y_
            dW2 = (a1.T @ err) / n + self.l2 * self.params["W2"]
            db2 = err.mean(axis=0, keepdims=True)
            da1 = err @ self.params["W2"].T
            dz1 = da1 * (1.0 - np.tanh(z1) ** 2)
            dW1 = (X.T @ dz1) / n + self.l2 * self.params["W1"]
            db1 = dz1.mean(axis=0, keepdims=True)
            self.params["W2"] -= self.lr * dW2
            self.params["b2"] -= self.lr * db2
            self.params["W1"] -= self.lr * dW1
            self.params["b1"] -= self.lr * db1
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        if not self.params:
            return np.zeros(len(X), dtype=float)
        z1 = X @ self.params["W1"] + self.params["b1"]
        a1 = np.tanh(z1)
        pred = a1 @ self.params["W2"] + self.params["b2"]
        return pred.ravel()
