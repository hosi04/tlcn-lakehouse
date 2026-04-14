import torch
import torch.nn as nn


class Autoencoder(nn.Module):

    def __init__(self, input_size: int = 5):
        super().__init__()

        self.encoder = nn.Sequential(
            nn.Linear(input_size, 16),
            nn.ReLU(),
            nn.BatchNorm1d(16),
            nn.Linear(16, 8),
            nn.ReLU(),
            nn.Linear(8, 3),
        )

        self.decoder = nn.Sequential(
            nn.Linear(3, 8),
            nn.ReLU(),
            nn.BatchNorm1d(8),
            nn.Linear(8, 16),
            nn.ReLU(),
            nn.Linear(16, input_size),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.decoder(self.encoder(x))

    def encode(self, x: torch.Tensor) -> torch.Tensor:
        return self.encoder(x)

    def reconstruction_error(self, x: torch.Tensor) -> torch.Tensor:
        return ((x - self.forward(x)) ** 2).mean(dim=1)
