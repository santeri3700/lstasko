# lstasko - List Taskomatic tasks
lstasko is a utility/module for fetching information about [Taskomatic](https://documentation.suse.com/external-tree/en-us/suma/4.0/suse-manager/architecture/taskomatic.html) tasks.

This project was inspired by [taskotop](https://github.com/uyuni-project/uyuni/blob/master/utils/taskotop).

## Status
lstasko requires Python 3.6 or higher and is only tested with the following systems:
- Uyuni Server 2022.08 (Python 3.6.15 & PostgreSQL 14 + SSL)
- Uyuni Server 2022.08 (PostgreSQL 14 + SSL) and a remote machine with Python 3.10 running lstasko.

This project is currently in early development stage and the code may change drastically. \
Contributions, feedback and feature requests are welcome!

## Version and changes
- **Version:** 0.1.0
- **Change logs:** [CHANGELOG.md](CHANGELOG.md)

## Features
- Get list of Taskomatic tasks directly from the database
- Get task details by task id
- Get Repo-sync Software Channel labels and ids
- Filter tasks by name, status and max-age
- Select output columns/fields
- Output with [Tabulate](https://github.com/astanin/python-tabulate) formatting (default stdout)
- Output in JSON (stdout)

## TODO
- Add ordering per column/field (ASC/DESC).
- Replace `_hack_parse_reposync_data()` with something more robust and capable of listing flags (no-errata, latest, sync-kickstart, fail).
- CI/CD.
- Better Exceptions and Exception handling.
- Better documentation.
- Continuous/Follow mode like in `taskotop` or `tail -F`.
- Maybe rename the project/module/library before PyPI release (taskoinfo, taskomatic-info etc..)
- Windows/MacOS compatibility (untested at the moment).
- Oracle DB compatibility(?).

# Installation

Dependencies:
- `psycopg2-binary==2.9.5` (for PostgreSQL database connections)
- `dataclasses==0.8.0` (required by tabulate)
- `tabulate==0.8.10` (for human-readable output)
- `colorlog==6.7.0` (for colorful log output)

## Install from source
```
git clone -b dev https://github.com/santeri3700/lstasko.git
cd lstasko
python3 ./setup.py build
sudo python3 ./setup.py install
```

# Usage
## CLI
```
lstasko --help

lstasko \
--status finished \
--max-age 3600 \
--name channel-repodata \
--columns id name start_time end_time \
--json

sudo lstasko \
--rhn-conf /etc/rhn/rhn.conf \
--status finished \
--max-age -1 \
--name repo-sync \
--columns id name start_time end_time duration data \
--output-format rst

lstasko \
--connection-string 'host=uyuni-server.example.com dbname=uyunidb user=uyuni password=SuperSecretPassword' \
--status running \
--max-age -1 \
--columns id name start_time end_time duration data bunch_desc \
--output-format github
```

**Available columns**: id, org_id, name, class, status, created, start_time, end_time, job_label, cron_expr, bunch_id, bunch_name, bunch_desc, bunch_org, data, stdout_file, stderr_file, duration

## Library
See [examples](examples) for more examples.
```py
import json
from lstasko import LSTasko

# Fetch connection string from Uyuni/SUSE Manager/Spacewalk/Satellite rhn.conf
db_conn_str = LSTasko().get_rhn_db_conn_str('/etc/rhn/rhn.conf')  # Default path

# Alternatively write a connection string manually
# Docs: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
db_conn_str = "host=uyuni-server.example.com dbname=uyunidb user=uyuni password=SuperSecretPassword sslmode=verify-full sslrootcert=root.pem"

with LSTasko(db_conn_str) as lstasko:
    # Fetch a list of all tasks
    all_tasks = lstasko.get_all_tasks()

    # Get task details per task id
    lstasko.get_task(12345)  # dict
    lstasko.get_task([12345, 12346])  # list[dict]

    # Get Software Channel id(s) from label(s)
    lstasko.get_channel_id('centos7-x86_64')  # int(123)
    lstasko.get_channel_id(['centos7-x86_64', 'centos7-x86_64-updates'])  # [{'channel_id': 123, 'channel_label': 'centos7-x86_64'}, ...]

    # Get Software Channel label(s) from id(s)
    lstasko.get_channel_label(123)  # str('centos7-x86_64')
    lstasko.get_channel_label([123, 124])  # [{'channel_id': 123, 'channel_label': 'centos7-x86_64'}, ...]
```

# Disclaimer

Copyright (C) 2022 Santeri Pikarinen (santeri3700)

This project is not affiliated with or endorsed by SUSE, The Uyuni Project, Red Hat or The Spacewalk Project.

This program is free software; you can redistribute it and/or modify \
it under the terms of the GNU General Public License version 2 published by \
the Free Software Foundation.

This program is distributed in the hope that it will be useful, \
but WITHOUT ANY WARRANTY; without even the implied warranty of \
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. \
See [LICENSE](LICENSE) for more details.
