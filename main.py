import os
import signal
import sys
import time

from processes import ForecastPipelineProcess


def sigterm_handler():
    # Raises SystemExit(0):
    sys.exit(0)


def lambda_handler(event, context):
    TIMEOUT = int(os.environ["TIMEOUT"])
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.alarm(TIMEOUT)

    start = time.time()
    try:
        forecastpipeline = ForecastPipelineProcess()
        # forecastpipeline.process()
        forecastpipeline.forecast()
        duration = time.time() - start
        print('Duration: %.2f' % duration)
    except:
        duration = time.time() - start
        print('Duration: %.2f' % duration)
        raise


