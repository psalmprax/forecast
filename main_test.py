import os
import signal
import sys
import time

from processes_test import ForecastPipelineProcess


def sigterm_handler():
    # Raises SystemExit(0):
    sys.exit(0)


def lambda_handler(event, context):
    signal.signal(signal.SIGTERM, sigterm_handler)
    TIMEOUT = int(os.environ["TIMEOUT"])
    start = time.time()
    try:
        forecastpipeline = ForecastPipelineProcess()
        # forecastpipeline.process()
        # forecastpipeline.forecast()
        data = forecastpipeline.process()[1]
        for i, row in data.iterrows():
            if (context.get_remaining_time_in_millis() / 1000) <= 10:
                signal.alarm(0)
            print(row)

        duration = time.time() - start

        print('Duration: %.2f' % duration)
    except:
        duration = time.time() - start
        print('Duration: %.2f' % duration)
        raise
