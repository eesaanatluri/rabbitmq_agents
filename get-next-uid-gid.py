#!/usr/bin/env python
import sys
import json
import ldap
import logging
import argparse
from os import popen
from rc_rmq import RCRMQ

task = 'get_next_uid_gid'

# Instantiate rabbitmq object
rc_rmq = RCRMQ({'exchange': 'RegUsr', 'exchange_type': 'topic'})

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true', help='verbose output')
parser.add_argument('-n', '--dry-run', action='store_true', help='enable dry run mode')
args = parser.parse_args()

#Default Log level
log_lvl = logging.WARNING

if args.verbose:
   log_lvl = logging.DEBUG
if args.dry_run:
   log_lvl = logging.INFO
  
# Logger
logging.basicConfig(format='%(asctime)s %(levelname)s [%(module)s] - %(message)s', level=log_lvl)
logger = logging.getLogger(__name__)

    
#Check if the username already exists via LDAP
def user_exists(username):
    try:
        logger.info(f"Searching LDAP for the user: {username}")
        con = ldap.initialize('ldap://ldapserver')
        ldap_base = "dc=cm,dc=cluster"
        query = "(uid={})".format(username)
        result = con.search_s(ldap_base, ldap.SCOPE_SUBTREE, query)
        logging.debug(f"The search result is: {result}")
        return result
    except ldap.LDAPError:
        logger.exception("Fatal LDAP error:")

# Define your callback function
def get_next_uid_gid(ch, method, properties, body):

    # Retrieve message
    msg = json.loads(body)
    logger.info("Received {}".format(msg))
    username = msg['username']
    success = False

    # Determine next available UID
    try:
        #if user_exists(username):
        if False:
            logger.info("The user, {} already exists".format(username))
            sys.exit(1)

        cmd_uid = "/usr/bin/getent passwd | \
            awk -F: '($3>10000) && ($3<20000) && ($3>maxuid) { maxuid=$3; } END { print maxuid+1; }'"
        if not args.dry_run:
            msg['uid'] = popen(cmd_uid).read().rstrip()

        logger.info(f"UID query: {cmd_uid}")

        cmd_gid = "/usr/bin/getent group | \
            awk -F: '($3>10000) && ($3<20000) && ($3>maxgid) { maxgid=$3; } END { print maxgid+1; }'"
        if not args.dry_run:
            msg['gid'] = popen(cmd_gid).read().rstrip()

        logger.info(f"GID query: {cmd_gid}")
        success = True
    except Exception:
        logger.exception("Fatal error:")

    # Acknowledge message
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # Send confirm message
    logger.debug('rc_rmq.publish_msg()')
    rc_rmq.publish_msg({
        'routing_key': 'confirm.' + username,
        'msg': {
            'task': task,
            'success': success
        }
    })
    logger.info('confirmation sent')

    if success:
        # Send create message to BrightCM agent
        logger.debug(f'The task {task} finished successfully')
        rc_rmq.publish_msg({
            'routing_key': 'create.' + username,
            'msg': msg
        })
        logger.info('create msg sent to next agent')

logger.info("Start listening to queue: {}".format(task))
rc_rmq.start_consume({
    'queue': task,
    'routing_key': "request.*",
    'cb': get_next_uid_gid
})

logger.info("Disconnected")
rc_rmq.disconnect()
