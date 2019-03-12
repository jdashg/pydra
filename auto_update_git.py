#!/usr/bin/env python3
assert __name__ == '__main__'

import subprocess
import sys
import threading
import time

# -

CHECK_INTERVAL = 60.0
PRETEND_UPDATE = False

args = sys.argv[1:]
while True:
    cur = args.pop()
    if cur == '--pretend':
        PRETEND_UPDATE = True
        continue
    if cur == '--secs':
        CHECK_INTERVAL = float(args.pop())
        continue
    args.append(cur)
    break

SUB_ARGS = args

# -

def git_rev(rev):
    return subprocess.check_output(['git', 'rev-parse', rev]).strip()

# -

should_restart = True

def th_kill_on_update(p):
    global should_restart

    first_time = True
    while True:
        if not first_time: # Skip the first wait.
            time.sleep(CHECK_INTERVAL)
        first_time = False

        try:
            rbranch = subprocess.check_output([
                    'git', 'rev-parse', '--abbrev-ref', '@{upstream}']).strip().decode()
        except subprocess.CalledProcessError:
            continue
        (remote, rbranch) = rbranch.split('/', 1)

        try:
            # TODO: git fetch --progress
            subprocess.run(['git', 'fetch', remote, rbranch], check=True, capture_output=True) # Mute output.
        except subprocess.CalledProcessError:
            continue
        head = git_rev('HEAD')
        fetch_head = git_rev('FETCH_HEAD')

        if head == fetch_head and not PRETEND_UPDATE:
            continue

        break

    print('[auto_update_git] Downloaded update {}->{}, restarting...'.format(head, fetch_head))
    p.terminate()
    should_restart = True

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
