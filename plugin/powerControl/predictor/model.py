import torch
import torch.nn as nn
import torch.nn.functional as F
import math


class MultiHeadSelfAttention(nn.Module):
    def __init__(self, channels, num_heads=8):
        super().__init__()
        assert channels % num_heads == 0

        self.num_heads = num_heads
        self.head_dim = channels // num_heads

        self.query = nn.Conv1d(channels, channels, 1)
        self.key = nn.Conv1d(channels, channels, 1)
        self.value = nn.Conv1d(channels, channels, 1)
        self.gamma = nn.Parameter(torch.tensor([0.0]))
        self.softmax = nn.Softmax(dim=-1)

    def forward(self, x):
        B, C, L = x.size()

        q = (
            self.query(x).view(B, self.num_heads, self.head_dim, L).permute(0, 1, 3, 2)
        )  # B, H, L, D
        k = self.key(x).view(B, self.num_heads, self.head_dim, L)  # B, H, D, L
        v = self.value(x).view(B, self.num_heads, self.head_dim, L)  # B, H, D, L

        energy = torch.matmul(q, k) / math.sqrt(self.head_dim)  # B, H, L, L
        attention = self.softmax(energy)  # B, H, L, L

        out = torch.matmul(attention, v.permute(0, 1, 3, 2))  # B, H, L, D
        out = out.permute(0, 1, 3, 2).contiguous().view(B, C, L)  # B, C, L

        out = self.gamma * out + x

        return out


class TransformerBlock(nn.Module):
    def __init__(self, d_model, nhead, dim_feedforward=2048, dropout=0.1):
        super().__init__()
        self.self_attn = nn.MultiheadAttention(
            d_model, nhead, dropout=dropout, batch_first=True
        )
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)
        self.dropout = nn.Dropout(dropout)

        self.linear1 = nn.Linear(d_model, dim_feedforward)
        self.linear2 = nn.Linear(dim_feedforward, d_model)
        self.activation = nn.GELU()

    def forward(self, x):
        attn_output, _ = self.self_attn(x, x, x)
        x = x + self.dropout(attn_output)
        x = self.norm1(x)

        ff_output = self.linear2(self.dropout(self.activation(self.linear1(x))))
        x = x + self.dropout(ff_output)
        x = self.norm2(x)

        return x


class NodePredictorNN(nn.Module):
    """
    Neural network for predicting node resource requirements.
    
    This model combines three types of inputs:
    1. past_hour: Time series data of recent resource usage (LSTM processed)
    2. cur_datetime: Current date/time features (hour of day, day of week, etc.)
    3. dayback: Historical resource usage patterns from previous days
    
    The model architecture consists of:
    - Bidirectional LSTM for processing temporal features
    - Multi-head self-attention for capturing relationships in the sequence
    - MLP encoders for datetime and dayback features
    - Fusion layer to combine all feature representations
    - Final predictor that outputs a value between 0 and 1000
    
    Args:
        feature_size: Number of features in the past_hour input
    """
    def __init__(self, feature_size):
        super().__init__()

        hidden_size = 128
        self.lstm = nn.LSTM(
            input_size=feature_size,
            hidden_size=hidden_size,
            num_layers=2,
            batch_first=True,
            bidirectional=True,
            dropout=0.2,
        )

        self.past_projection = nn.Sequential(
            nn.Linear(hidden_size * 2, 256), 
            nn.ReLU(),
            nn.BatchNorm1d(256),
            nn.Dropout(0.2),
        )

        self.attention = MultiHeadSelfAttention(256, num_heads=4)

        self.datetime_encoder = nn.Sequential(
            nn.Linear(3, 64),
            nn.ReLU(),
            nn.BatchNorm1d(64),
            nn.Dropout(0.2),
            nn.Linear(64, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128),
            nn.Dropout(0.2),
        )

        self.dayback_encoder = nn.Sequential(
            nn.Linear(4, 64),
            nn.ReLU(),
            nn.BatchNorm1d(64),
            nn.Dropout(0.2),
            nn.Linear(64, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128),
            nn.Dropout(0.2),
        )

        self.fusion = nn.Sequential(
            nn.Linear(256 + 128 + 128, 256),
            nn.ReLU(),
            nn.BatchNorm1d(256),
            nn.Dropout(0.2),
        )

        self.predictor = nn.Sequential(
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128),
            nn.Dropout(0.2),
            nn.Linear(128, 1),
            nn.ReLU(),
        )

    def forward(self, past_hour, cur_datetime, dayback):
        lstm_out, _ = self.lstm(past_hour)  # [B, L, H*2]

        last_hidden = lstm_out[:, -1, :]  # [B, H*2]
        past_features = self.past_projection(last_hidden)  # [B, 256]

        attention_in = past_features.unsqueeze(-1)  # [B, 256, 1]
        past_features = self.attention(attention_in).squeeze(-1)  # [B, 256]

        datetime_features = self.datetime_encoder(cur_datetime)  # [B, 128]
        dayback_features = self.dayback_encoder(dayback)  # [B, 128]

        combined = torch.cat(
            [past_features, datetime_features, dayback_features], dim=1
        )
        fused = self.fusion(combined)

        pred = self.predictor(fused)
        pred = torch.clamp(pred, min=0.0, max=1000.0)

        return pred
