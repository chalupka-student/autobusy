
import numpy as np
import pandas as pd

import klasyfikacja_nn as klas_nn
import predykcja_nn as pred_nn
import pickle


def main():
    # Dla każdej grupy 1-3 oraz liczby epok (10, 20, 30, 50, 100)
    for group in [1, 2, 3]:
        group_file = f'labeled_selected_routes_group_{group}.csv'
        for epochs in [10, 20, 30, 50, 100]:
            # klasyfikacja - bez cross-walidacji
            wyniki = klas_nn.klasyfikacja_nn(
                group_file,
                f'grupa_{group}_klasyfikacja_bez_cv_{epochs}_epoch',
                epochs=epochs
            )
            # klasyfikacja - 5 fold cross-walidacja
            wyniki = klas_nn.klasyfikacja_nn_cv(
                group_file,
                f'grupa_{group}_klasyfikacja_5cv_{epochs}_epoch',
                epochs=epochs
            )
            # predykcja - bez cross-walidacji
            wyniki = pred_nn.predykcja_nn(
                group_file,
                f'grupa_{group}_bez_cv_{epochs}_epoch',
                epochs=epochs
            )
            # predykcja - 5 fold cross-walidacja
            wyniki = pred_nn.predykcja_nn_cv(
                group_file,
                f'grupa_{group}_5cv_{epochs}_epoch',
                epochs=epochs
            )



def test():
    # wyniki=klas_nn.klasyfikacja_nn('labeled_selected_routes_group_1.csv','test',epochs=10)
    wyniki=pred_nn.predykcja_nn('labeled_selected_routes_group_1.csv','test',epochs=10)

main()