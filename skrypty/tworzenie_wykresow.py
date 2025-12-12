import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay, precision_score, recall_score
import pickle

def main():
    for group in [1,2,3]:
        for epoch in [10,20,30,50,100]:
            # objs = []
            # with open(f'data/wyniki_predykcji/grupa_{group}_bez_cv_{epoch}_epoch','rb') as f:
            #     while True:
            #         try:
            #             objs.append(pickle.load(f))
            #         except EOFError:
            #             break
            # wyniki=objs[-1]
            # df_results=wyniki[2]
            # df_results = df_results.sort_values('orig_idx', ascending=True).set_index('orig_idx')
            # df_results=df_results.reset_index()

            # plt.figure(figsize=(12,5))
            # plt.plot(df_results.index,df_results["y_true"],label="Prawdziwe wartości")
            # plt.plot(df_results.index,df_results["y_pred"],label="Predykcje", alpha=0.7)
            # plt.legend()
            # plt.title(f'Wykres prawdziwych wartości i predykcji grupy {group}, {epoch} epok')
            # plt.xlabel('Numer próbki')
            # plt.ylabel('Wartość opóźnienia (min)')
            # plt.savefig(f'obrazy/wykresy_predykcji/prawdziwe_predykcje/PrawdziwePredykcje_grupa_{group}_{epoch}_epok')
            # plt.close()


            # median_by_route = df_results.groupby('route_name').median(numeric_only=True)[['y_true', 'y_pred']]
            # median_by_route = median_by_route.sort_values('y_true', ascending=False)
            # plt.figure(figsize=(10,6))
            # width = 0.35
            # routes = median_by_route.index
            # x = np.arange(len(routes))
            # plt.bar(x - width/2, median_by_route['y_true'], width, label='Mediana rzeczywistych')
            # plt.bar(x + width/2, median_by_route['y_pred'], width, label='Mediana przewidywanych')
            # plt.xlabel('Nazwa linii')
            # plt.ylabel('Mediana opóźnienia (min)')
            # plt.title(f'Mediana opóźnień (predykcji i rzeczywistych) dla poszczególnych linii, grupa {group}, {epoch} epok')
            # plt.xticks(x, routes, rotation=90)
            # plt.legend()
            # plt.tight_layout()
            # plt.savefig(f'obrazy/wykresy_predykcji/predykcje_mediany/PredykcjeMediany_grupa_{group}_{epoch}_epok')
            # plt.close()


            # diff=abs(df_results["y_true"] - df_results["y_pred"])
            # quantile_075=diff.quantile(0.75)
            # quantile_09=diff.quantile(0.9)
            # plt.figure(figsize=(12,5))
            # plt.scatter(df_results.index, diff,alpha=0.5,s=1,label='|y_pred - y_true|')
            # plt.hlines(quantile_075,0,df_results.shape[0],'k',label='kwantyl 0.75')
            # plt.hlines(quantile_09,0,df_results.shape[0],'r',label='kwantyl 0.9')
            # plt.xlabel("Numer próbki")
            # plt.ylabel('|y_pred - y_true| (min)')
            # plt.title(f"Wartość bezwzględna z różnic wartości prawdziwych i przewidywanych, grupa {group}, {epoch} epok")
            # plt.axhline(0, color='gray', linestyle='--', linewidth=1)
            # plt.legend()
            # plt.minorticks_on()
            # plt.grid(which='both',alpha=0.1)
            # plt.savefig(f'obrazy/wykresy_predykcji/predykcje_roznice/PredykcjeRoznice_grupa_{group}_{epoch}_epok')
            # plt.close()


            # plt.figure(figsize=(8,6))
            # plt.plot(wyniki[0],label='Błąd trenowania')
            # plt.plot(wyniki[1],label='Błąd testowania')
            # plt.title(f'Wartości błędu trenowania i testowania w zależności od epoki, grupa {group}, {epoch} epok')
            # plt.xlabel('Numer epoki')
            # plt.ylabel('Błąd')
            # plt.grid()
            # plt.legend()
            # plt.savefig(f'obrazy/wykresy_predykcji/predykcje_traintest/PredykcjeTrainTest_grupa_{group}_{epoch}_epok')
            # plt.close()


            # objs = []
            # with open(f'data/wyniki_predykcji/grupa_{group}_5cv_{epoch}_epoch','rb') as f:
            #     while True:
            #         try:
            #             objs.append(pickle.load(f))
            #         except EOFError:
            #             break
            # wyniki=objs[-1]
            # wyniki['fold_preds'][0]
            # for i in range(5):
            #     wyniki['fold_preds'][i] = wyniki['fold_preds'][i].sort_values('orig_idx', ascending=True).set_index('orig_idx')
            #     wyniki['fold_preds'][i]=wyniki['fold_preds'][i].reset_index()
            
            # for i in range(5):
            #     predykcje=wyniki['fold_preds'][i]
            #     plt.figure(figsize=(12,5))
            #     plt.plot(predykcje.index,predykcje["y_true"], label="Prawdziwe wartości")
            #     plt.plot(predykcje.index,predykcje["y_pred"], label="Predykcje", alpha=0.7)
            #     plt.ylabel('Wartość opóźnienia (min)')
            #     plt.xlabel('Numer próbki')
            #     plt.legend()
            #     plt.title(f'Predykcje w foldzie nr. {i+1}, grupa {group}, {epoch} epok')
            #     plt.savefig(f'obrazy/wykresy_predykcji_cv/prawdziwe_predykcje/PrawdziwePredykcje_fold_{i}_grupa_{group}_{epoch}_epok')
            #     plt.close()

            #     diff=abs(predykcje["y_true"] - predykcje["y_pred"])
            #     quantile_075=diff.quantile(0.75)
            #     quantile_09=diff.quantile(0.9)
            #     plt.figure(figsize=(10,5))
            #     plt.scatter(predykcje.index, diff,alpha=0.5,s=1,label='|y_pred - y_true|')
            #     plt.hlines(quantile_075,0,predykcje.shape[0],'k',label='kwantyl 0.75')
            #     plt.hlines(quantile_09,0,predykcje.shape[0],'r',label='kwantyl 0.9')
            #     plt.xlabel("Numer próbki")
            #     plt.ylabel('|y_pred - y_true| (min)')
            #     plt.title(f"Wartość bezwzględna z różnic wartości prawdziwych i przewidywanych, fold {i}, grupa {group}, {epoch} epok")
            #     plt.axhline(0, color='gray', linestyle='--', linewidth=1)
            #     plt.legend()
            #     plt.minorticks_on()
            #     plt.grid(which='both',alpha=0.1)
            #     plt.savefig(f'obrazy/wykresy_predykcji_cv/predykcje_traintest/PredykcjeTrainTest_fold_{i}_grupa_{group}_{epoch}_epok')
            #     plt.close()



            objs = []
            with open(f'data/wyniki_predykcji/grupa_{group}_klasyfikacja_bez_cv_{epoch}_epoch','rb') as f:
                while True:
                    try:
                        objs.append(pickle.load(f))
                    except EOFError:
                        break
            wyniki_zapis=objs[-1]
            df_results = wyniki_zapis[2]
            df_results = df_results.sort_values('orig_idx', ascending=True).set_index('orig_idx')
            df_results=df_results.reset_index()

            cm = confusion_matrix(df_results['y_true'], df_results['y_pred'])
            classes = sorted(df_results['y_true'].unique())
            disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=classes)
            fig, ax = plt.subplots(figsize=(7, 5))
            disp.plot(ax=ax, cmap='Blues')
            plt.title(f"Macierz pomyłek (wartości), grupa {group}, {epoch} epok")
            plt.xlabel("Predykowane klasy")
            plt.ylabel("Prawdziwe klasy")
            plt.savefig(f'obrazy/wykresy_klasyfikacji/klasyfikacje_macierze_wartości/MacierzKonfuzji_wartości_grupa_{group}_{epoch}_epok')
            plt.close()


            cm_normalized = confusion_matrix(df_results['y_true'], df_results['y_pred'], normalize='true')
            disp_norm = ConfusionMatrixDisplay(confusion_matrix=cm_normalized, display_labels=classes)
            fig, ax = plt.subplots(figsize=(7, 5))
            disp_norm.plot(ax=ax, cmap='Blues', values_format=".2f")
            plt.title(f"Macierz pomyłek (znormalizowana) grupa {group}, {epoch} epok")
            plt.xlabel("Predykowane klasy")
            plt.ylabel("Prawdziwe klasy")
            plt.savefig(f'obrazy/wykresy_klasyfikacji/klasyfikacje_macierze_normalne/MacierzKonfuzji_normalne_grupa_{group}_{epoch}_epok')
            plt.close()


                        
            plt.figure(figsize=(8,6))
            plt.plot(wyniki_zapis[0],label='Błąd trenowania')
            plt.plot(wyniki_zapis[1],label='Błąd testowania')
            plt.title(f'Wartości błędu trenowania i testowania klasyfikacji w zależności od epoki, grupa {group}, {epoch} epok')
            plt.xlabel('Numer epoki')
            plt.ylabel('Błąd')
            plt.legend()
            plt.savefig(f'obrazy/wykresy_klasyfikacji/klasyfikacje_traintest/KlasyfikacjeTrainTest_grupa_{group}_{epoch}_epok')
            plt.close()


            objs = []
            with open(f'data/wyniki_predykcji/grupa_{group}_klasyfikacja_5cv_{epoch}_epoch','rb') as f:
                while True:
                    try:
                        objs.append(pickle.load(f))
                    except EOFError:
                        break

            wyniki=objs[-1]
            for i in range(5):
                wyniki['fold_preds'][i] = wyniki['fold_preds'][i].sort_values('orig_idx', ascending=True).set_index('orig_idx')
                wyniki['fold_preds'][i]=wyniki['fold_preds'][i].reset_index()

            for i in range(5):
                predykcje = wyniki['fold_preds'][i]
                cm = confusion_matrix(predykcje['y_true'], predykcje['y_pred'])
                classes = sorted(predykcje['y_true'].unique())
                disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=classes)
                fig, ax = plt.subplots(figsize=(7, 5))
                disp.plot(ax=ax, cmap='Blues')
                plt.title(f"Macierz pomyłek (wartości), fold {i}, grupa {group}, {epoch} epok")
                plt.xlabel("Predykowane klasy")
                plt.ylabel("Prawdziwe klasy")
                plt.savefig(f'obrazy/wykresy_klasyfikacji_cv/klasyfikacje_macierze_wartości_cv/MacierzKonfuzji_wartości_fold_{i}_grupa_{group}_{epoch}_epok')
                plt.close()

                cm_norm = confusion_matrix(predykcje['y_true'], predykcje['y_pred'], normalize='true')
                disp_norm = ConfusionMatrixDisplay(confusion_matrix=cm_norm, display_labels=classes)
                fig, ax = plt.subplots(figsize=(7, 5))
                disp_norm.plot(ax=ax, cmap='Blues', values_format='.2f')
                plt.title(f"Macierz pomyłek (znormalizowana), fold {i}, grupa {group}, {epoch} epok")
                plt.xlabel("Predykowane klasy")
                plt.ylabel("Prawdziwe klasy")
                plt.savefig(f'obrazy/wykresy_klasyfikacji_cv/klasyfikacje_macierze_normalne_cv/MacierzKonfuzji_normalne_fold_{i}_grupa_{group}_{epoch}_epok')
                plt.close()


main()