
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np


class LSTMAutoencoder(nn.Module):
    """
    LSTM Autoencoder for Anomaly Detection in Time Series (PyTorch Version).
    Architecture:
        Input(Window) -> Encoder(LSTM) -> Latent(Vector) -> Decoder(LSTM) -> Output(Window)
    """

    def __init__(self, window_size, n_features, latency_dim=3, hidden_dim=64, num_layers=1, dropout=0.2):
        super(LSTMAutoencoder, self).__init__()
        self.window_size = window_size
        self.n_features = n_features
        self.latency_dim = latency_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Encoder
        self.encoder = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0
        )
        self.dropout_layer = nn.Dropout(dropout)

        # Latent compression
        self.to_latent = nn.Linear(hidden_dim, latency_dim)
        self.from_latent = nn.Linear(latency_dim, hidden_dim)

        # Decoder
        self.decoder = nn.LSTM(
            input_size=hidden_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0
        )

        # Output layer
        self.output_layer = nn.Linear(hidden_dim, n_features)

        # Initialize Weights
        self._init_weights()
        self.to(self.device)

    def _init_weights(self):
        """Xavier Initialization for better convergence."""
        for name, param in self.named_parameters():
            if 'weight' in name:
                nn.init.xavier_normal_(param)
            elif 'bias' in name:
                nn.init.constant_(param, 0.0)

    def forward(self, x):
        # Encoder
        _, (hidden_n, _) = self.encoder(x)
        last_hidden = hidden_n[-1]
        last_hidden = self.dropout_layer(last_hidden)  # Apply Dropout

        # Latent
        latent = self.to_latent(last_hidden)
        latent = nn.functional.leaky_relu(latent)  # Non-linearity for complex patterns

        # Decoder Prep
        hidden_restored = self.from_latent(latent)
        # Repeat for each time step
        repeated_hidden = hidden_restored.unsqueeze(1).repeat(1, self.window_size, 1)

        # Decoder
        dec_out, _ = self.decoder(repeated_hidden)

        # Reconstruction
        reconstructed = self.output_layer(dec_out)
        return reconstructed, latent  # Return latent for visualization

    def save_checkpoint(self, path, epoch, optimizer, loss):
        """Saves full checkpoint with metadata."""
        torch.save({
            'epoch': epoch,
            'model_state_dict': self.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'loss': loss,
            'config': {
                'window_size': self.window_size,
                'n_features': self.n_features,
                'hidden_dim': self.hidden_dim,
                'latent_dim': self.latency_dim
            }
        }, path)
        print(f"  💾 Checkpoint saved: {path}")

    def train_model(self, X_train, X_val, epochs=50, batch_size=64, lr=1e-3,
                    patience=15, scheduler_patience=3, scheduler_factor=0.5,
                    save_path="models/lstm_ae_champion.pth", noise_factor=0.0):
        """
        Trains the model with Limit-aware Learning Rate Scheduler and Early Stopping.
        Uses Lazy Loading and Validation Batching to prevent OOM.
        """
        # Convert to Tensor but KEEP ON CPU initially
        train_tensor = torch.from_numpy(X_train.astype(np.float32))
        val_tensor = torch.from_numpy(X_val.astype(np.float32))

        train_loader = DataLoader(TensorDataset(train_tensor, train_tensor), batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(TensorDataset(val_tensor, val_tensor), batch_size=batch_size, shuffle=False)

        criterion = nn.L1Loss()
        optimizer = optim.Adam(self.parameters(), lr=lr)
        scheduler = optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode='min', factor=scheduler_factor, patience=scheduler_patience, verbose=True
        )

        best_loss = float('inf')
        counter = 0
        history = {'train_loss': [], 'val_loss': []}

        print(f"🚀 Device: {self.device}")

        for epoch in range(epochs):
            self.train()
            train_loss = 0

            for batch_x, target_x in train_loader:
                # Move to GPU Just-In-Time
                batch_x = batch_x.to(self.device)
                target_x = target_x.to(self.device)

                # Apply Denoising Noise (if enabled)
                if noise_factor > 0:
                    noise = torch.randn_like(batch_x) * noise_factor
                    batch_x = batch_x + noise

                optimizer.zero_grad()
                output, _ = self(batch_x)
                loss = criterion(output, target_x)
                loss.backward()
                nn.utils.clip_grad_norm_(self.parameters(), max_norm=1.0)
                optimizer.step()
                train_loss += loss.item()

            avg_train_loss = train_loss / len(train_loader)

            # Validation (Batched)
            self.eval()
            val_loss_accum = 0
            with torch.no_grad():
                for val_batch, _ in val_loader:
                    val_batch = val_batch.to(self.device)
                    val_out, _ = self(val_batch)
                    batch_loss = criterion(val_out, val_batch).item()
                    val_loss_accum += batch_loss

            avg_val_loss = val_loss_accum / len(val_loader)

            history['train_loss'].append(avg_train_loss)
            history['val_loss'].append(avg_val_loss)

            print(f"Epoch [{epoch + 1}/{epochs}] Train Loss: {avg_train_loss:.6f} | Val Loss: {avg_val_loss:.6f}")

            # Update Scheduler
            scheduler.step(avg_val_loss)

            # Checkpoint
            if avg_val_loss < best_loss:
                best_loss = avg_val_loss
                self.save_checkpoint(save_path, epoch, optimizer, best_loss)
                counter = 0
            else:
                counter += 1
                if counter >= patience:
                    print(f"🛑 Early Stopping triggered after {patience} epochs without improvement.")
                    break

        return history

    def predict(self, X):
        self.eval()
        tensor_X = torch.from_numpy(X.astype(np.float32)).to(self.device)
        dataset = TensorDataset(tensor_X)
        loader = DataLoader(dataset, batch_size=256, shuffle=False)

        predictions = []
        with torch.no_grad():
            for batch in loader:
                batch_x = batch[0]
                out, _ = self(batch_x)
                predictions.append(out.cpu().numpy())

        return np.concatenate(predictions, axis=0)

    def get_reconstruction_error(self, X):
        """
        Calculates MSE for each sample window.
        Returns: Array of shape (n_samples,) containing the MSE score.
        """
        X_pred = self.predict(X)
        mse = np.mean(np.square(X - X_pred), axis=(1, 2))
        return mse
