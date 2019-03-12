#!/usr/bin/env python3
assert __name__ == '__main__'

import signal
import subprocess
import sys
import threading
import time

# -

CHECK_INTERVAL = 60.0
PRETEND_UPDATE = False

SUB_ARGS = sys.argv[1:]

# -

def git_head():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()

# -

should_restart = True

def th_kill_on_update(p):
    global should_restart
    start_head = git_head()
    while True:
        try:
            stdout = subprocess.check_output(['git', 'pull', '--no-ff']) # Squelch stdout.
        except CalledProcessError:
            sys.stdout.buffer.write(stdout)
            break
        pull_head = git_head()

        if pull_head == start_head and not PRETEND_UPDATE:
            time.sleep(CHECK_INTERVAL)
            continue

        print('[auto_update_git] Updated {}->{}, restarting...'.format(
                start_head, pull_head))
        should_restart = True
        break
    p.terminate()
    return

# -

try:
    while should_restart:
        should_restart = False

        # Update or fail early.
        p = subprocess.run(['git', 'pull', '--no-ff'], capture_output=True)
        if p.returncode:
            sys.stdout.buffer.write(p.stdout)
            sys.stderr.buffer.write(p.stderr)
            if b'Permission denied' in p.stderr:
                push_url = subprocess.check_output(['git', 'remote', 'get-url', 'origin']).strip()
                sys.stderr.write('''
If your upstream fetch url is not unauthenticated https, try:
  git remote set-url origin https://github.com/jdashg/pydra.git
  git remote set-url --push origin {}'''.format(push_url.decode()))
            exit(1)


        p = subprocess.Popen(SUB_ARGS)
        threading.Thread(target=th_kill_on_update, args=(p,), daemon=True).start()
        p.wait()
    exit(p.returncode)
except KeyboardInterrupt:
    # KeyboardInterrupt seems to be sent to child processes too, so we don't have to do
    # anything special to propagate the signal.
    pass
