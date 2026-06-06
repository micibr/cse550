import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from torchao.quantization import quantize_, Int8DynamicActivationInt8WeightConfig

# 1. Generate Training Data
np.random.seed(42)
NUM_SAMPLES = 2000
x_values = np.random.uniform(low=0, high=2 * np.pi, size=NUM_SAMPLES).astype(np.float32)
np.random.shuffle(x_values)
y_values = np.sin(x_values).astype(np.float32)

# Split into train (80%) and test (20%)
split = int(NUM_SAMPLES * 0.8)
x_train, x_test = x_values[:split], x_values[split:]
y_train, y_test = y_values[:split], y_values[split:]

x_train_t = torch.from_numpy(x_train).unsqueeze(1)
y_train_t = torch.from_numpy(y_train).unsqueeze(1)
x_test_t  = torch.from_numpy(x_test).unsqueeze(1)
y_test_t  = torch.from_numpy(y_test).unsqueeze(1)

train_loader = DataLoader(TensorDataset(x_train_t, y_train_t), batch_size=32, shuffle=True)

# 2. Define the Neural Network Architecture
class SineModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(1, 16),
            nn.ReLU(),
            nn.Linear(16, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
        )

    def forward(self, x):
        return self.net(x)

model = SineModel()

# 3. Train the Model
optimizer = torch.optim.Adam(model.parameters())
criterion = nn.MSELoss()

for epoch in range(200):
    model.train()
    for xb, yb in train_loader:
        optimizer.zero_grad()
        criterion(model(xb), yb).backward()
        optimizer.step()

    if (epoch + 1) % 20 == 0:
        model.eval()
        with torch.no_grad():
            val_loss = criterion(model(x_test_t), y_test_t).item()
        print(f"Epoch {epoch + 1}/200 — val_loss: {val_loss:.6f}")

# 4. Post-Training Quantization (Int8) via torchao eager-mode API
model.eval()
quantize_(model, Int8DynamicActivationInt8WeightConfig())

torch.save(model, "sine_model_quantized.pt")
print("Quantized model saved successfully!")
