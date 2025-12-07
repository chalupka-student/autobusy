
import numpy as np
import pandas as pd

import klasyfikacja_nn as klas_nn
import predykcja_nn as pred_nn
import pickle


def main():
    wyniki=klas_nn.klasyfikacja_nn('labeled_selected_routes_group_1.csv','grupa_1_klasyfikacja_bez_cv_100_epoch',epochs=100)
    wyniki=klas_nn.klasyfikacja_nn_cv('labeled_selected_routes_group_1.csv','grupa_1_klasyfikacja_5cv_100_epoch',epochs=100)
    wyniki=pred_nn.predykcja_nn('labeled_selected_routes_group_1.csv','grupa_1_bez_cv_100_epoch',epochs=100)
    wyniki=pred_nn.predykcja_nn_cv('labeled_selected_routes_group_1.csv','grupa_1_5cv_100_epoch',epochs=100)

    wyniki=klas_nn.klasyfikacja_nn('labeled_selected_routes_group_2.csv','grupa_2_klasyfikacja_bez_cv_100_epoch',epochs=100)
    wyniki=klas_nn.klasyfikacja_nn_cv('labeled_selected_routes_group_2.csv','grupa_2_klasyfikacja_5cv_100_epoch',epochs=100)
    wyniki=pred_nn.predykcja_nn('labeled_selected_routes_group_2.csv','grupa_2_bez_cv_100_epoch',epochs=100)
    wyniki=pred_nn.predykcja_nn_cv('labeled_selected_routes_group_2.csv','grupa_2_5cv_100_epoch',epochs=100)

    wyniki=klas_nn.klasyfikacja_nn('labeled_selected_routes_group_3.csv','grupa_3_klasyfikacja_bez_cv_100_epoch',epochs=100)
    wyniki=klas_nn.klasyfikacja_nn_cv('labeled_selected_routes_group_3.csv','grupa_3_klasyfikacja_5cv_100_epoch',epochs=100)
    wyniki=pred_nn.predykcja_nn('labeled_selected_routes_group_3.csv','grupa_3_bez_cv_100_epoch',epochs=100)
    wyniki=pred_nn.predykcja_nn_cv('labeled_selected_routes_group_3.csv','grupa_3_5cv_100_epoch',epochs=100)

def test():
    wyniki=klas_nn.klasyfikacja_nn('labeled_selected_routes_group_1.csv','test',epochs=10)
    wyniki=pred_nn.predykcja_nn('labeled_selected_routes_group_1.csv','test',epochs=10)

main()