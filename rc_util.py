import logging
import argparse
from rc_rmq import RCRMQ
import json

rc_rmq = RCRMQ({'exchange': 'RegUsr', 'exchange_type': 'topic'})
tasks = {'ohpc_account': None, 'ood_account': None, 'slurm_account': None}
logger_fmt = '%(asctime)s [%(module)s] - %(message)s'

def add_account(username, email, full='', reason=''):
  rc_rmq.publish_msg({
    'routing_key': 'request.' + username,
    'msg': {
      "username": username,
      "email": email,
      "fullname": full,
      "reason": reason
    }
  })
  rc_rmq.disconnect()

def worker(ch, method, properties, body):
    msg = json.loads(body)
    task = msg['task']
    tasks[task] = msg['success']
    print("Got msg: {}({})".format(msg['task'], msg['success']))

    # Check if all tasks are done
    done = True
    for key, status in tasks.items():
        if not status:
            print("{} is not done yet.".format(key))
            done = False
    if done:
        rc_rmq.stop_consume()
        rc_rmq.delete_queue()

def consume(username, routing_key='', callback=worker, debug=False):
    if routing_key == '':
        routing_key = 'confirm.' + username

    if debug:
        sleep(5)
    else:
        rc_rmq.start_consume({
            'queue': username,
            'routing_key': routing_key,
            'cb': callback
        })
        rc_rmq.disconnect()

    return { 'success' : True }

def get_args():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument('-n', '--dry-run', action='store_true', help='enable dry run mode')
    return parser.parse_args()

def get_logger(args=None):
    if args is None:
        args = get_args()

    logger_lvl = logging.WARNING

    if args.verbose:
        logger_lvl = logging.DEBUG

    if args.dry_run:
        logger_lvl = logging.INFO

    logging.basicConfig(format=logger_fmt, level=logger_lvl)
    return logging.getLogger(__name__)

