from rc_rmq import RCRMQ
import json

rc_rmq = RCRMQ({'exchange': 'RegUsr', 'exchange_type': 'topic'})
tasks = {'get_next_uid_gid':'','bright_account':'','subscribe_mail_list':''}

def add_account(username, email, full='', reason='', uid='', gid=''):
  rc_rmq.publish_msg({
    'routing_key': 'request.' + username,
    'msg': {
      "username": username,
      "email": email,
      "fullname": full,
      "reason": reason,
      "uid": uid,
      "gid": gid
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

def consume(username, callback=worker, debug=False):
    if debug:
        sleep(5)
    else:
        rc_rmq.start_consume({
            'queue': username,
            'routing_key': 'confirm.' + username,
            'cb': callback
        })
        rc_rmq.disconnect()

    return { 'success' : True }
