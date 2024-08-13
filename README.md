# gdelthon
Mateam's repo for Factored's 2024 Datathon, on GDELT Data


# Virtual environment setup

Highly recommended!

1.  Verify python version is 3.10. If not, download that version of Python (but not too recent, e.g. NOT 3.12 or 3.13 as some libraries might not be available for the newest versions of Python)

```bash
python3 --version
```

2. Create virtual env (from the repo root!)

```shell
python3 -m venv .venv/
```

3. Activate virtual env

```bash
# On Mac/Linux
source .venv/bin/activate
# On Windows, it is something like .venv/bin/ see documentation
```

4. Install requirements

```
pip install -r requirements.txt
```


# Profile.env setup (ENV variables)

Highly recommended.

On Linux/Mac, create a file called `profile.env`, directly under your home/user directory (NOT in the code directory) to hold your env vars.

```bash
# EXAMPLE contents of file
export DATA_DIR='/home/teo/data'   # GDELT data files will be stored here
export PYTHONPATH='./'  # So that python can resolve imports such as `from data_proc....`
```

On Windows, define these variables directly using the Windows system app for that...

