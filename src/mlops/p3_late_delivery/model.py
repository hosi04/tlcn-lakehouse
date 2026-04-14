import torch
import torch.nn as nn


class LateDeliveryLSTM(nn.Module):

    def __init__(
        self,
        input_size:  int  = 5,  
        hidden_size: int  = 32,
        num_layers:  int  = 1,
        dropout:     float = 0.2,
    ):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size,
            hidden_size,
            num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.fc      = nn.Linear(hidden_size, 1)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out    = self.dropout(out[:, -1, :])   
        return self.sigmoid(self.fc(out))       
