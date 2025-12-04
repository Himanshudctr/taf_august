import logging
from logging import DEBUG
import datetime
from pyspark.tests.test_readwrite import InputFormatTests

timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")

logging.basicConfig(
    filename=f'app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# N D I W E C
# N - No logging
# D - DEBUG
# I - INFO
# W - WARNING
# E - ERROR
# C - CRITICAL


logging.debug('this is debug message')  #default is infor in the setting that debug
logging.info('Program started')
logging.warning('Low disk space')
logging.error('File not found!')
logging.critical('this is critical')

print("this is debug message")
print("Program started")
print("Low disk space")
print("File not found!")
print("this is critical")
