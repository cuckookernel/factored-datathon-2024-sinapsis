"""Test loading raw data"""


import data_proc.load as ld

# %%

def _interactive_testing() -> None:
    # %%
    from importlib import reload

    from shared import runpyfile
    reload(ld)
    runpyfile("test/test_load.py")
    # %%
