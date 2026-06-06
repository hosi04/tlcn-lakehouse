import torch
import torch.nn as nn


class RevenueLSTM(nn.Module):
    def __init__(
        self,
        input_size:    int   = 4,
        hidden_size:   int   = 128,
        num_layers:    int   = 2,
        dropout:       float = 0.2,
        bidirectional: bool  = False,
        mlp_hidden:    int   = 64,
    ):
        super().__init__()
        self.hidden_size   = hidden_size
        self.num_layers    = num_layers
        self.bidirectional = bidirectional
        self.directions    = 2 if bidirectional else 1

        self.lstm = nn.LSTM(
            input_size  = input_size,
            hidden_size = hidden_size,
            num_layers  = num_layers,
            batch_first = True,
            dropout     = dropout if num_layers > 1 else 0.0,
            bidirectional = bidirectional,
        )

        lstm_out_size = hidden_size * self.directions

        self.norm    = nn.LayerNorm(lstm_out_size)
        self.dropout = nn.Dropout(dropout)

        self.head = nn.Sequential(
            nn.Linear(lstm_out_size, mlp_hidden),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(mlp_hidden, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        h0 = torch.zeros(
            self.num_layers * self.directions,
            x.size(0),
            self.hidden_size,
            device=x.device,
        )
        c0 = torch.zeros_like(h0)

        out, _ = self.lstm(x, (h0, c0))
        out     = out[:, -1, :]
        out     = self.norm(out)
        out     = self.dropout(out)
        return self.head(out)
