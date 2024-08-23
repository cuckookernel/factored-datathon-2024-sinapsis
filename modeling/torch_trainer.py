"""Training with torch"""
from collections import defaultdict
from collections.abc import Callable
from time import perf_counter

import torch
from torch import Tensor, nn
from torch.optim import Optimizer
from torch.utils.data import DataLoader

from shared import logging

L = logging.getLogger("tch_trn")


class Trainer:
    """Somewhat generic Supervise ML model trainer"""

    def __init__(self, model: nn.Module, *,
                 batch_proc_fn: Callable[[dict[str, Tensor]], tuple[Tensor, Tensor]],
                 loss_fn: Callable[[Tensor, Tensor], Tensor],

                 optimizer: Optimizer) -> None:
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        L.info(f"torch device={self.device.type!r}")
        self.model = model.to(self.device)
        self.loss_fn = loss_fn
        self.optimizer = optimizer
        self.log_dict: dict[str, list[float]] = defaultdict(list)
        self.batch_proc_fn = batch_proc_fn

    def fit(self,
            train_loader: DataLoader,
            valid_loader: DataLoader,
            num_epochs: int,
            n_epochs_log: int = 10) -> nn.Module:
        """Run n_pochs of fit"""
        start_time = perf_counter()

        for epoch in range(num_epochs):
            start_time_epoch = perf_counter()
            self.model.train()

            train_losses: list[Tensor] = []
            for batch in train_loader:
                loss = self._batch_loss(batch)
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                train_losses.append(loss.detach())

            if epoch==0 or (epoch + 1) % n_epochs_log == 0:
                train_loss = torch.hstack(train_losses).mean()
                valid_loss = self.compute_valid_loss(valid_loader)
                now = perf_counter()

                L.info('**Epoch: %3d/%3d'
                       ' | train_loss=%.3f | valid_loss=%.3f'
                       ' | epoch_time=%.1f s | total_time=%.1f s',
                       epoch + 1, num_epochs,
                       train_loss.item(), valid_loss.item(),
                       now - start_time_epoch, now - start_time )

                self.log_dict['train_loss_per_epoch'].append(train_loss.item())
                self.log_dict['valid_loss_per_epoch'].append(valid_loss.item())

        L.info('Training complete!')
        return self.model.eval()


    def compute_valid_loss(self, valid_loader: DataLoader) -> Tensor:
        """Compute the validation loss"""
        self.model.eval()

        with torch.set_grad_enabled(False):  # save memory during inference
            losses: list[float] = [self._batch_loss(batch).item()
                                   for batch in valid_loader]

        return torch.tensor(losses).mean()


    def _batch_loss(self, batch: dict[str, Tensor]) -> Tensor:
        input_, truth = self.batch_proc_fn(batch)
        input_, truth = input_.to(self.device), truth.to(self.device)
        output = self.model(input_)
        return self.loss_fn(output, truth)
