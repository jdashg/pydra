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
        subprocess.check_call(['git', 'pull', '--no-ff']) # Update or fail early.

        p = subprocess.Popen(SUB_ARGS)
        threading.Thread(target=th_kill_on_update, args=(p,), daemon=True).start()
        p.wait()
    exit(p.returncode)
except KeyboardInterrupt:
    # KeyboardInterrupt seems to be sent to child processes too, so we don't have to do
    # anything special to propagate the signal.
    pass
