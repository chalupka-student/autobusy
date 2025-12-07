
import numpy as np
import pandas as pd

import klasyfikacja_nn as klas_nn
import predykcja_nn as pred_nn
import pickle


def main():
    wyniki=klas_nn.klasyfikacja_nn('labeled_selected_routes_group_3.csv','grupa_3_klasyfikacja_bez_cv_20_epoch',epochs=20)
    wyniki=klas_nn.klasyfikacja_nn_cv('labeled_selected_routes_group_3.csv','grupa_3_klasyfikacja_5cv_20_epoch',epochs=20)
    wyniki=pred_nn.predykcja_nn('labeled_selected_routes_group_3.csv','grupa_3_bez_cv_30_epoch',epochs=30)
    wyniki=pred_nn.predykcja_nn_cv('labeled_selected_routes_group_3.csv','grupa_3_5cv_30_epoch',epochs=30)

main()