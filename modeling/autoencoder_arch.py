"""Autoencoder classes"""
from torch import Tensor, nn

# %%

# Define the autoencoder model
class Autoencoder(nn.Module):
    """A simple autoencoder based on linear layers and relus"""

    N_ENCODER_LAYERS = 3

    def __init__(self, layer_dims: list[int]) -> None:
        """Construct the network"""
        super().__init__()

        if len(layer_dims) != Autoencoder.N_ENCODER_LAYERS + 1:
            raise ValueError(f"Expecting layer_dims to have exactly "
                             f"{Autoencoder.N_ENCODER_LAYERS + 1}")

        self.encoder = nn.Sequential(
            nn.Linear(layer_dims[0], layer_dims[1]),
            nn.LeakyReLU(0.01),
            nn.Linear(layer_dims[1], layer_dims[2]),
            nn.LeakyReLU(0.01),
            nn.Linear(layer_dims[2], layer_dims[3]),
        )
        self.decoder = nn.Sequential(
            nn.Linear(layer_dims[3], layer_dims[2]),
            nn.LeakyReLU(0.01),
            nn.Linear(layer_dims[2], layer_dims[1]),
            nn.LeakyReLU(0.01),
            nn.Linear(layer_dims[1], layer_dims[0]),
            # nn.Tanh()
        )


    def encode(self, x: Tensor) -> Tensor:
        """Just encode"""
        return self.encoder(x)


    def forward(self, x: Tensor) -> Tensor:
        """Encode and decode"""
        encoded = self.encoder(x)
        return self.decoder(encoded)
