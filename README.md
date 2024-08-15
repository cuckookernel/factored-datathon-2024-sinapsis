# Team Sinapsis - Factored's Datathon 2024

Team Members

  - Tomas Zubik - (github: [tomaszbk](https://github.com/tomaszbk)) - [linkedin](https://www.linkedin.com/in/tomas-zubik/) - Argentina 
  - William Mateus - (github: [willmateusav](https://github.com/willmateusav)) - [linkedin](https://www.linkedin.com/in/william-mateus-avila-5a762ba3/) - Colombia 
  - Adrian Rojas Fernández - (github:[flakoash](https://github.com/flakoash) ) - [linkedin](https://www.linkedin.com/in/adrian-rojas-fernandez/) - Bolivia
  - Mateo Restepo - (github: [cuckookernel](https://github.com/cuckookernel)) - [linkedin](https://www.linkedin.com/in/mateorestrepo/) Colombia

# Useful links

Data Sources:
- GDELT 1.0 GKG: http://data.gdeltproject.org/gkg/index.html
- Dataset description: http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf

- GDELT 1.0 Events: http://data.gdeltproject.org/events/index.html
- Dataset description: http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf
- Data Headers: https://www.gdeltproject.org/data/lookups/CSV.header.dailyupdates.txt

# Data Pipeline Architecture Prototype

- Apache Airflow / databricks jobs
- Databricks, medallion architecture, amazon s3
- Data Quality assessment with Soda, great expectations or Delta Live Tables
- Data governance, including security, access, discoverability, and documentation (?)

# Virtual environment setup

Highly recommended!

1.  Verify python version is 3.10. If not, download that version of Python (but not too recent, e.g. NOT 3.12 or 3.13 as some libraries might not be available for the newest versions of Python)
S's repo for Factored's 2024 Datathon, on GDELT Data

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
export S3_PREFIX='s3://databricks-workspace-stack-41100-bucket/unity-catalog/2578366185261862'
```

On Windows, define these variables directly using the Windows system app for that...
