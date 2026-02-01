import torch
import torch.nn as nn
import numpy as np
import os

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
        self.device = torch.device("cpu") # Demo runs on CPU

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
        
        self.to(self.device)

    def forward(self, x):
        # Encoder
        _, (hidden_n, _) = self.encoder(x)
        last_hidden = hidden_n[-1]
        last_hidden = self.dropout_layer(last_hidden) 

        # Latent
        latent = self.to_latent(last_hidden)
        latent = nn.functional.leaky_relu(latent) 

        # Decoder Prep
        hidden_restored = self.from_latent(latent)
        repeated_hidden = hidden_restored.unsqueeze(1).repeat(1, self.window_size, 1)

        # Decoder
        dec_out, _ = self.decoder(repeated_hidden)

        # Reconstruction
        reconstructed = self.output_layer(dec_out)
        return reconstructed, latent 

def load_lstm_model(checkpoint_path):
    """
    Load LSTM Autoencoder from .pth checkpoint.
    Returns: model (torch.nn.Module), config (dict)
    """
    if not os.path.exists(checkpoint_path):
        raise FileNotFoundError(f"Model not found at {checkpoint_path}")
        
    print(f"Loading LSTM from {checkpoint_path}...")
    checkpoint = torch.load(checkpoint_path, map_location=torch.device('cpu'))
    
    config = checkpoint.get('config', {
        'window_size': 60,
        'n_features': 6, # Default
        'hidden_dim': 64,
        'latent_dim': 3
    })
    
    model = LSTMAutoencoder(
        window_size=config['window_size'],
        n_features=config['n_features'],
        latency_dim=config.get('latent_dim', 3),
        hidden_dim=config.get('hidden_dim', 64)
    )
    
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    
    return model, config
