import pandas as pd
from statsmodels.api import OLS
import statsmodels.api as sm
import numpy as np


def perform_regression_analysis(df):
    Y = df["rating_value"]
    X = df[["setting_ind"]]
    X = sm.add_constant(X)
    model = OLS(Y, X).fit()
    return model


def evaluate_model(model, data):
    # Placeholder for evaluation logic
    pass


def human_model_alignment(human_scores, model_scores):
    correlation = np.corrcoef(human_scores, model_scores)[0, 1]
    return correlation
