<p align="center">
  <a href="" rel="noopener">
 <img  src="img/sparkify.png" alt="Project logo"></a>
</p>

<h3 align="center">Sparkify Datalake</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]() [![GitHub Issues](https://img.shields.io/github/issues/kellermann92/sparkify-lake.svg)](https://github.com/kellermann92/sparkify-lake/issues) [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/kellermann92/sparkify-lake.svg)](https://github.com/kellermann92/sparkify-lake/pulls) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<center> </center>
<p align="center"> Processing .json files to build a parquet table with Spark.
    <br> 
</p>

---
## 📝 Table of Contents

- [About](#about)
- [Getting Started](#getting_started)

- [Built Using](#built_using)

- [Authors](#authors)


## About <a name = "about"></a>

Since our platform growth exponentially in the past few weeks we are not able to store `songplays` table and its sources on a data warehouse. So our engineering team was asked to develop a pipeline to generate and populate the `songplays` table and its sources in a datalake in a`S3` bucket.

## 🏁 Getting Started <a name = "getting_started"></a>
The data we will be working on is stored in two `S3` buckets.
* **Log data**: contains users events on platform and have the following columns: 

>artist, auth, firstName, gender, itemInSession, lastName, length, level location, method, page, registration, sessionId, song, status, ts, userAgent

The song dataset on the other hand is a `json` file with the structure presented in the example bellow:
```json
{
  "num_songs": 1,
  "artist_id": "ARJIE2Y1187B994AB7",
  "artist_latitude": null,
  "artist_longitude": null,
  "artist_location": "",
  "artist_name": "Line Renaud",
  "song_id": "SOUPIRU12A6D4FA1E1",
  "title": "Der Kleine Dompfaff",
  "duration": 152.92036,
  "year": 0
}
```

### Prerequisites

To execute this project you'll need a `dl.cfg` file containing an AWS access and secret ID for an IAM User role with permissions of write and read an `S3` bucket. The content of this file must be in the following format:

```text
[AWS]
AWS_ACCESS_KEY_ID=A******************5
AWS_SECRET_ACCESS_KEY=E**************************************S
```

You will need to install [`Python 3.6.3`](https://www.python.org/downloads/release/python-363/) with `PySpark 2.4.3` installed. After installing `Python`, you can install `PySpark` by running the following command:

```powershell
pip install pyspark==2.4.3
```
> * In order to run `PySpark 2.4.3`, you will need to install [`Java JDK 8`](https://www.oracle.com/br/java/technologies/javase/javase-jdk8-downloads.html). 
> * We strongly recomment to use a specific virtual environment in Python to execute this project. Click [here](https://docs.python.org/3/tutorial/venv.html) to know more about virtual environments in Python or [here](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) to know more about virtual environments in Anaconda.
> 
### Installing

To execute this project you only need to download this repository, activate the virtual environment with `Python 3.6.3` and `PySpark 2.4.3` in it and run the following command:
```
python etl.py
```
> * You can run this code in test mode without accessing the `S3`bucket. To do this you just have to uncomment the lines `280` and `281` and comment the lines `277` and `278` in `etl.py`.
> * The test files are in `input` and `output` subirectories in this repository.

After a few seconds you should see the following lines in your command line:
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
YY/MM/DD HH:MM:SS WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
[Stage 2:===============================>                       (116 + 8) / 200]YY/MM/DD HH:MM:SS WARN MemoryManager: Total allocation exceeds 95,00% (1.020.054.720 bytes) of heap memory
```
If the file was successfully finished you should see a sample of the final table:
```
############
Songplays table:
+------------+--------------------+-------+-----+------------------+------------------+----------+--------------------+--------------------+
|songplays_id|          start_time|user_id|level|           song_id|         artist_id|session_id|            location|          user_agent|
+------------+--------------------+-------+-----+------------------+------------------+----------+--------------------+--------------------+
|           0|2018-11-21 19:56:...|     15| paid|SOZCTXZ12AB0182364|AR5KOSW1187FB35FF4|       818|Chicago-Napervill...|"Mozilla/5.0 (X11...|
+------------+--------------------+-------+-----+------------------+------------------+----------+--------------------+--------------------+

############
```
## ⛏️ Built Using <a name = "built_using"></a>

- [PySpark](https://spark.apache.org/docs/2.4.3/) - Cluster computing system.
## ✍️ Authors <a name = "authors"></a>

- [@kellermann92](https://github.com/kellermann92) - Idea & Initial work

