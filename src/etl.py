'''
Filename: /home/kellermann/git_repo/sparkify-lake/etl.py
Path: /home/kellermann/git_repo/sparkify-lake
Created Date: Wednesday, March 31st 2021, 10:35:11 am
Author: Leandro Kellermann de Oliveira

Copyright (c) 2021 myProjects
'''

import configparser
# from datetime import datetime
import os
from session.spark_session import create_spark_session
from nodes.log_data import process_log_data
from nodes.song_data import process_song_data


def main():
    """Main method to execute the ETL process."""

    # Config variables are meant to be used when you set the .cfg file with AWS credentials.
    # Tbh it is a bad idea if you use AWS. It is better to use proper KMS authentication or
    # IAM Role.

    # But at the time I saw this for the first time... looked like a good idea,

    # config = configparser.ConfigParser()
    # config.read('dl.cfg')
    # os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    # os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    session = create_spark_session()

    # input = 's3a://udacity-dend/'
    # output = 's3a://mkt-sparkify/'

    # You can use the sample dataset in this repository to test the application:
    my_input = 'input/'
    my_output = 'output/'
    process_song_data(session, my_input, my_output)
    process_log_data(session, my_input, my_output)

    print('Finish!')


if __name__ == "__main__":
    main()
