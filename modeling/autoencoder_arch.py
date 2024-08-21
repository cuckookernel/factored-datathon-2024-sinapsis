
from torch import nn, Tensor


# %%

# Define the autoencoder model
class Autoencoder(nn.Module):
    """"""
    N_ENCODER_LAYERS = 3

    def __init__(self, layer_dims: list[int]) -> None:
        """Construct the network"""
        super(Autoencoder, self).__init__()

        if len(layer_dims) != Autoencoder.N_ENCODER_LAYERS + 1:
            raise ValueError(f"Expecting layer_dims to have exactly "
                             f"{Autoencoder.N_ENCODER_LAYERS + 1}")

        self.encoder = nn.Sequential(
            nn.Linear(layer_dims[0], layer_dims[1]),
            nn.LeakyReLU(0.01),
            nn.Linear(layer_dims[1], layer_dims[2]),
            nn.LeakyReLU(0.01),
            nn.Linear(layer_dims[2], layer_dims[3])
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
        return self.encoder(x)


    def forward(self, x: Tensor) -> Tensor:
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)

        return decoded
