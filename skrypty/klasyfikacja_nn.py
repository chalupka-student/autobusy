import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader, random_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
import pandas as pd
import numpy as np
from sklearn.model_selection import KFold

def klasyfikacja_nn(df_name, classification=True, epochs=30, batch_size=256, lr=1e-3):

    # ---- 1. Wczytanie danych ----
    delays_labeled = pd.read_csv(df_name)
    if 'Unnamed: 0' in delays_labeled.columns:
        delays_labeled = delays_labeled.drop(columns='Unnamed: 0')

    delays_labeled['day'] = delays_labeled['day'].astype(int)
    df = delays_labeled.copy()

    # ---- 2. Kolumny ----
    categorical_cols = ['stop_sequence', 'route_name']
    numeric_cols = ['day', 'sin_time', 'cos_time']
    target_col = 'delay_label'

    # ---- 3. Enkodowanie kategorycznych (tylko do indeksów, nie jako cechy wejściowe) ----
    label_encoders = {} 
    cat_cardinalities = []
    for col in categorical_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        label_encoders[col] = le
        cat_cardinalities.append(df[col].nunique())

    # ---- 4. Standaryzacja cech numerycznych ----
    scaler = StandardScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

    # ---- 5. Przygotowanie tensorów ----
    X_cats = df[categorical_cols].values.astype(np.int64)
    X_nums = df[numeric_cols].values.astype(np.float32)
    y = df[target_col].values

    if classification:
        y = y.astype(int)
    else:
        y = y.astype(np.float32)

    # ---- 6. Dataset PyTorch ----
    class DelayDataset(Dataset):
        def __init__(self, X_cats, X_nums, y):
            self.X_cats = torch.tensor(X_cats, dtype=torch.long)
            self.X_nums = torch.tensor(X_nums, dtype=torch.float32)
            self.y = torch.tensor(y)
        def __len__(self):
            return len(self.y)
        def __getitem__(self, idx):
            return self.X_cats[idx], self.X_nums[idx], self.y[idx]

    dataset = DelayDataset(X_cats, X_nums, y)
    train_size = int(0.8 * len(dataset))
    test_size = len(dataset) - train_size
    train_ds, test_ds = random_split(dataset, [train_size, test_size])
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_ds, batch_size=batch_size)

    # ---- 7. Sieć neuronowa z embeddingami ----
    class DelayNet(nn.Module):
        def __init__(self, cat_cardinalities, num_numeric, output_dim):
            super().__init__()
            # embedding layers
            self.embeddings = nn.ModuleList([
                nn.Embedding(num_categories, min(50, (num_categories + 1) // 2))
                for num_categories in cat_cardinalities
            ])
            emb_dim_total = sum(e.embedding_dim for e in self.embeddings)
            input_dim = emb_dim_total + num_numeric

            self.net = nn.Sequential(
                nn.Linear(input_dim, 128),
                nn.ReLU(),
                nn.Linear(128, 64),
                nn.ReLU(),
                nn.Linear(64, output_dim)
            )

        def forward(self, x_cat, x_num):
            embs = [emb(x_cat[:, i]) for i, emb in enumerate(self.embeddings)]
            x = torch.cat(embs + [x_num], dim=1)
            return self.net(x)

    output_dim = 1 if not classification else len(np.unique(y))
    device = torch.device("cuda")
    model = DelayNet(cat_cardinalities, X_nums.shape[1], output_dim).to(device)

    # ---- 8. Ustawienie kryterium i optymalizatora ----
    criterion = nn.MSELoss() if not classification else nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)

    train_losses = []
    test_losses = []

    # ---- 9. Trening ----
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        for x_cat, x_num, yb in train_loader:
            x_cat, x_num, yb = x_cat.to(device), x_num.to(device), yb.to(device)
            optimizer.zero_grad()
            preds = model(x_cat, x_num).view(-1)
            loss = criterion(preds, yb) if not classification else criterion(preds, yb.long())
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
        avg_train_loss = total_loss / len(train_loader)

        # Ewaluacja
        model.eval()
        total_test_loss = 0
        with torch.no_grad():
            for x_cat, x_num, yb in test_loader:
                x_cat, x_num, yb = x_cat.to(device), x_num.to(device), yb.to(device)
                preds = model(x_cat, x_num).squeeze()
                loss = criterion(preds, yb) if not classification else criterion(preds, yb.long())
                total_test_loss += loss.item()
        avg_test_loss = total_test_loss / len(test_loader)

        train_losses.append(avg_train_loss)
        test_losses.append(avg_test_loss)
        print(f"Epoch {epoch+1}/{epochs} | Train Loss: {avg_train_loss:.4f} | Test Loss: {avg_test_loss:.4f}")

        # ---- 9. Generowanie predykcji dla zbioru testowego ----
        model.eval()
        preds_list = []
        y_list = []
        route_list = []

        
        with torch.no_grad():
            for idx, (x_cat, x_num, yb) in enumerate(test_loader):
                x_cat, x_num = x_cat.to(device), x_num.to(device)

                # predykcja
                preds = model(x_cat, x_num).squeeze().cpu().numpy()
                preds_list.extend(preds)

                # prawdziwe wartości
                y_list.extend(yb.cpu().numpy())

                # pobieramy indeksy z datasetu i odczytujemy route_name z oryginalnego dataframe
                batch_start = idx * test_loader.batch_size
                batch_end = batch_start + len(yb)
                original_indices = test_ds.indices[batch_start:batch_end]

                route_batch = df.iloc[original_indices]["route_name"].values
                route_list.extend(route_batch)

        # Zamiana na tablicę
        df_results = pd.DataFrame({
            "route_name": route_list,
            "y_true": y_list,
            "y_pred": preds_list})

    return {
        "model": model,
        "train_losses": train_losses,
        "test_losses": test_losses,
        'results':df_results,

        "label_encoders": label_encoders,
        "scaler": scaler
    }



def klasyfikacja_nn_cv(df_name, classification=True, epochs=30, batch_size=256, lr=1e-3, n_splits=5):

    # ---- 1. Wczytanie danych ----
    delays_labeled = pd.read_csv(df_name)
    if 'Unnamed: 0' in delays_labeled.columns:
        delays_labeled = delays_labeled.drop(columns='Unnamed: 0')

    delays_labeled['day'] = delays_labeled['day'].astype(int)
    df = delays_labeled.copy()

    # ---- 2. Kolumny ----
    categorical_cols = ['stop_sequence', 'route_name']
    numeric_cols = ['day', 'sin_time', 'cos_time']
    target_col = 'delay_label'

    # ---- 3. Enkodowanie kolumn kategorycznych ----
    label_encoders = {}
    cat_cardinalities = []
    for col in categorical_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        label_encoders[col] = le
        cat_cardinalities.append(df[col].nunique())

    # ---- 4. Standaryzacja ----
    scaler = StandardScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

    # ---- 5. Przygotowanie danych ----
    X_cats = df[categorical_cols].values.astype(np.int64)
    X_nums = df[numeric_cols].values.astype(np.float32)
    y = df[target_col].values.astype(int if classification else np.float32)

    device = torch.device("cuda")

    # ---- 6. Model bazowy ----
    class DelayNet(nn.Module):
        def __init__(self, cat_cardinalities, num_numeric, output_dim):
            super().__init__()
            self.embeddings = nn.ModuleList([
                nn.Embedding(num_categories, min(50, (num_categories + 1)//2))
                for num_categories in cat_cardinalities
            ])
            emb_dim_total = sum(e.embedding_dim for e in self.embeddings)
            input_dim = emb_dim_total + num_numeric
            self.net = nn.Sequential(
                nn.Linear(input_dim, 128),
                nn.ReLU(),
                nn.Linear(128, 64),
                nn.ReLU(),
                nn.Linear(64, output_dim)
            )

        def forward(self, x_cat, x_num):
            embs = [emb(x_cat[:, i]) for i, emb in enumerate(self.embeddings)]
            x = torch.cat(embs + [x_num], dim=1)
            return self.net(x)

    # ---- 7. Dataset ----
    class DelayDataset(Dataset):
        def __init__(self, X_cats, X_nums, y):
            self.X_cats = torch.tensor(X_cats, dtype=torch.long)
            self.X_nums = torch.tensor(X_nums, dtype=torch.float32)
            self.y = torch.tensor(y)
        def __len__(self): return len(self.y)
        def __getitem__(self, idx):
            return self.X_cats[idx], self.X_nums[idx], self.y[idx]

    # ---- 8. Cross-validation ----
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    fold_results = []
    fold_preds=[]

    for fold, (train_idx, val_idx) in enumerate(kf.split(X_cats)):
        print(f"\n===== Fold {fold+1}/{n_splits} =====")
        Xc_train, Xc_val = X_cats[train_idx], X_cats[val_idx]
        Xn_train, Xn_val = X_nums[train_idx], X_nums[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]

        train_ds = DelayDataset(Xc_train, Xn_train, y_train)
        val_ds = DelayDataset(Xc_val, Xn_val, y_val)
        train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_ds, batch_size=batch_size)

        model = DelayNet(cat_cardinalities, X_nums.shape[1], 1 if not classification else len(np.unique(y)))
        model.to(device)

        criterion = nn.MSELoss() if not classification else nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=lr)

        # Trening
        for epoch in range(epochs):
            model.train()
            total_loss = 0
            for x_cat, x_num, yb in train_loader:
                x_cat, x_num, yb = x_cat.to(device), x_num.to(device), yb.to(device)
                optimizer.zero_grad()
                preds = model(x_cat, x_num).squeeze()
                loss = criterion(preds, yb) if not classification else criterion(preds, yb.long())
                loss.backward()
                optimizer.step()
                total_loss += loss.item()
            avg_train_loss = total_loss / len(train_loader)
            print(f'Avg train loss in epoch {epoch}:{avg_train_loss:.4f}')

        model.eval()
        total_val_loss = 0
        with torch.no_grad():
            for x_cat, x_num, yb in val_loader:
                x_cat, x_num, yb = x_cat.to(device), x_num.to(device), yb.to(device)
                # --- predykcja bez niebezpiecznego squeeze() ---
                preds = model(x_cat, x_num)
                if not classification:
                    preds = preds.view(-1)        # (batch,) dla regresji
                # dla klasyfikacji preds ma shape (batch, n_classes) i nie zmieniamy
                loss = criterion(preds, yb) if not classification else criterion(preds, yb.long())
                total_val_loss += loss.item()
        avg_val_loss = total_val_loss / len(val_loader)
        fold_results.append(avg_val_loss)
        print(f"Fold {fold+1} | Train loss: {avg_train_loss:.4f} | Val loss: {avg_val_loss:.4f}")

        # ---- zbieranie predykcji po batchach (używamy val_idx zamiast val_ds.indices) ----
        model.eval()
        preds_list = []
        y_list = []
        route_list = []

        with torch.no_grad():
            for batch_i, (x_cat, x_num, yb) in enumerate(val_loader):
                x_cat, x_num = x_cat.to(device), x_num.to(device)

                # predykcja
                preds = model(x_cat, x_num)
                if not classification:
                    preds = preds.view(-1)   # wymuszamy kształt (batch,)
                preds_np = preds.cpu().numpy()
                preds_list.extend(preds_np)

                # prawdziwe wartości (na CPU)
                y_list.extend(yb.cpu().numpy())

                # mapowanie batch -> oryginalne indeksy w dataframe
                batch_start = batch_i * val_loader.batch_size
                batch_end = batch_start + len(yb)   # len(yb) bo ostatni batch może być mniejszy
                original_indices = val_idx[batch_start:batch_end]   # <-- używamy val_idx

                # jeśli chcesz oryginalne nazwy tras (stringi), weź z delays_labeled (przed enkodowaniem)
                route_batch = delays_labeled.iloc[original_indices]["route_name"].values
                route_list.extend(route_batch)

        df_results = pd.DataFrame({
            "route_name": route_list,
            "y_true": y_list,
            "y_pred": preds_list
        })

        fold_preds.append(df_results)

    mean_loss = np.mean(fold_results)
    print(f"\nŚredni wynik cross-walidacji ({n_splits}-fold): {mean_loss:.4f}")

    return {
        "cv_losses": fold_results,
        "mean_cv_loss": mean_loss,
        'fold_preds':fold_preds

    }