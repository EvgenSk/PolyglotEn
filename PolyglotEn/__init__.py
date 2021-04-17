import os
import sys
import azure.functions as func

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)
from functions import run

async def main(msg: func.ServiceBusMessage):
    await run(msg)
