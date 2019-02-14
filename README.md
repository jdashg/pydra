# pydra
Job remoter and distributer

## Configuration with `~/.pydra/config.py`

An example:
```
import logging

JOB_SERVER_ADDR = ('192.168.1.125', JOB_SERVER_ADDR[1])
LOG_LEVEL = logging.DEBUG
```

## `ccerb` Module

Compiles C/C++ objects remotely.

### Building Firefox

This commit is known to build the following Firefox commit:
```
Author: Daniel Varga <dvarga@mozilla.com>
Date:   Sat Feb 9 23:47:19 2019 +0200

    Merge mozilla-inbound to mozilla-central. a=merge
```

On Windows, you'll need Python 3 installed so that `py -3` works.

`~/.pydra/config.py`:
```
CC_LIST += [
    'C:\\Users\\MyUserName\\.mozbuild\\clang\\bin\\clang-cl.exe',
]
```

`.mozconfig`:
```
ac_add_options "--with-compiler-wrapper=py $topsrcdir/../../pydra/pydra ccerb"
```

*(I had weird issues with JS's moz.configure pass mis-parsing the compiler args when I explicitly used `--with-compiler-wrapper=py -3 [...]`)*
