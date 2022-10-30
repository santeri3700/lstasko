#
# Copyright (C) 2022 Santeri Pikarinen <santeri3700>
#
# This file is part of lstasko.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License version 2 for more details.
#
# You should have received a copy of the GNU General Public License version 2
# along with this program; if not, see <http://www.gnu.org/licenses/>.

import sys
import argparse
import datetime
import json
import logging
from .lstasko import LSTasko
from .exceptions import LSTaskoException, LSTaskoDatabaseNotConnectedException, LSTaskoNoRhnConfException

# Optional dependencies
try:
    from tabulate import tabulate
except ImportError:
    tabulate = None  # Tabulate is not installed

try:
    import colorlog
    logger = colorlog.getLogger('lstasko')
except ImportError:
    colorlog = None  # Colorlog is not installed
    logger = logging.getLogger('lstasko')


def main():
    # Arguments
    argparser = argparse.ArgumentParser(description="lstasko - List Taskomatic tasks")
    argparser.add_argument("-A", "--all", action="store_true",
                           dest="filter_show_all",
                           help="show all Taskomatic tasks (other filters still apply)")
    argparser.add_argument("-s", "--status", type=str.lower, nargs='+',
                           dest="filter_status", default='all',
                           help="filter tasks per status(es) (running, finished etc..)",
                           metavar="STATUS")
    argparser.add_argument("-n", "--name", type=str.lower, nargs='+',
                           dest="filter_name", default='all',
                           help="filter tasks per name(s)",
                           metavar="TASK_NAME")
    argparser.add_argument("-c", "--columns", type=str.lower, nargs='+',
                           dest="filter_columns", default='default',
                           help="select columns/fields for output",
                           metavar="COLUMN")
    argparser.add_argument("-m", "--max-age", type=int,
                           dest="filter_max_age", default=300,
                           help="filter out events older than given amount of seconds (default 300, -1 for unlimited)",
                           metavar="SECONDS")
    connection_group = argparser.add_mutually_exclusive_group()
    connection_group.add_argument("-cs", "--connection-string", type=str,
                                  dest="db_connection_string",
                                  help="PostgreSQL/libpq connection string",
                                  metavar="PGSQL_CONN_STR")
    connection_group.add_argument("-C", "--rhn-conf", "--rhn-config", type=str,
                                  dest="db_connection_conf", default="/etc/rhn/rhn.conf",
                                  help="rhn.conf for PostgreSQL/libpq connection (default: /etc/rhn/rhn.conf)",
                                  metavar="RHN_CONF_FILE")
    output_group = argparser.add_mutually_exclusive_group()
    output_group.add_argument("-F", "--output-format",
                              type=str.lower, dest="output_format",
                              help="output as human readable tabulate table (stdout)",
                              metavar="TABULATE_FMT")
    output_group.add_argument("-J", "--json", action="store_true",
                              dest="output_json", default=False,
                              help="output as JSON (stdout)")
    argparser.add_argument("-N", "--no-colors", action="store_false",
                           dest="output_colors",
                           help="disable colored log output if supported (stderr)")
    argparser.add_argument("-v", "--verbose", action="store_true",
                           dest="output_debug", default=False,
                           help="enable verbose debug output (stderr)")
    args = argparser.parse_args()

    # Logging
    try:
        log_level = logging.DEBUG if args.output_debug else logging.INFO
        if colorlog and args.output_colors:
            # Optional Colorlog support
            handler = colorlog.StreamHandler()
            if log_level == logging.DEBUG:
                handler.setFormatter(colorlog.ColoredFormatter(
                    '%(log_color)s[%(levelname)s] %(funcName)s - %(message)s',
                    log_colors={
                        'DEBUG':    'cyan',
                        'WARNING':  'yellow',
                        'ERROR':    'red',
                        'CRITICAL': 'bold_red'
                    })
                )
            else:
                handler.setFormatter(colorlog.ColoredFormatter(
                    '%(log_color)s[%(levelname)s] %(message)s',
                    log_colors={
                        'WARNING':  'yellow',
                        'ERROR':    'red',
                        'CRITICAL': 'bold_red',
                    })
                )
        else:
            # Fallback to regular logging
            handler = logging.StreamHandler()
            if log_level == logging.DEBUG:
                handler.setFormatter(
                    logging.Formatter(
                        '[%(levelname)s] %(funcName)s - %(message)s',
                    )
                )
            else:
                handler.setFormatter(
                    logging.Formatter(
                        '[%(levelname)s] %(message)s',
                    )
                )

        logger.setLevel(log_level)
        logger.addHandler(handler)

        # Disable tracebacks in non-debug mode
        if not logger.isEnabledFor(logging.DEBUG):
            sys.tracebacklimit = 0
    except Exception as e:
        sys.exit(f"Unknown error while initializing logger: {e}")

    # Get database connection string from rhn.conf (if available)
    db_conn_str = None
    if not args.db_connection_string:
        try:
            logger.debug(f"Attempting to open \"{args.db_connection_conf}\"...")
            db_conn_str = LSTasko().get_rhn_db_conn_str(args.db_connection_conf)
        except LSTaskoNoRhnConfException as e:
            logger.error(e)
            return False
        except Exception as e:
            logger.error(
                f"Unknown error: {e}\n"
                "Can't connect to database! Check PostgreSQL connection string or rhn.conf."
            )
            return False
    else:
        db_conn_str = str(args.db_connection_string)

    # Fetch and output information from Taskomatic database
    try:
        with LSTasko(db_conn_str) as lstasko:
            # Debug prints
            if not colorlog:
                logger.debug("Color output is disabled. Colorlog is not installed.")
            if not tabulate:
                logger.debug("Human readable output is disabled. Tabulate is not installed.")

            # Time
            now = datetime.datetime.now(tz=datetime.timezone.utc)

            # Taskomatic
            all_tasks = lstasko.get_all_tasks()
            all_tasks_details = []

            # Columns
            default_columns = ['id', 'status', 'name', 'start_time', 'end_time', 'duration', 'data']
            if args.filter_columns and args.filter_columns != 'default':
                columns = args.filter_columns
            else:
                columns = default_columns

            # Filter validations
            if args.filter_show_all:
                if args.filter_max_age == 300:
                    args.filter_max_age = -1

            if args.filter_max_age:
                if args.filter_max_age == -1:
                    args.filter_max_age = int(datetime.datetime.timestamp(now))
                elif args.filter_max_age < 0:
                    logger.error("Task max age must be a positive number in seconds or -1 for unlimited!")
                    return False

            # Filter and manipulate task data for output
            for task in all_tasks:

                # Max age
                if task['end_time'] and task['end_time'] < (now - datetime.timedelta(seconds=args.filter_max_age)):
                    # logger.debug(
                    #     f"Task [{task['id']}] \"{task['name']}\" is older than {args.filter_max_age}s! Skipping..."
                    # )
                    continue

                # Status
                if 'all' not in args.filter_status and str(task['status']).lower() not in args.filter_status:
                    logger.debug(
                        f"Task [{str(task['id']).lower()}] \"{task['name']}\" status \"{task['status']}\" " +
                        f"doesn't match filters: {args.filter_status}. Skipping...")
                    continue

                # Name
                if 'all' not in args.filter_name and str(task['name']).lower() not in args.filter_name:
                    logger.debug(
                        f"Task [{str(task['id']).lower()}] \"{task['name']}\" name " +
                        f"doesn't match filters: {args.filter_name}. Skipping...")
                    continue

                # Get repo-sync details (if available)
                if (task['name'] == 'repo-sync'):
                    channel_data = lstasko.get_reposync_details(task['data'])
                    if channel_data:
                        task['data'] = {
                            'channel_id': channel_data[0]['channel_id'],
                            'channel_label': channel_data[0]['channel_label']
                        }
                    else:
                        task['data'] = None  # Clear unparsed data

                # Calculate task duration
                if task['end_time']:
                    task['duration'] = (task['end_time'] - task['start_time']).seconds
                else:
                    task['duration'] = (now - task['start_time']).seconds

                # Make timestamps more user readable in non-JSON output
                for time_field in ['start_time', 'end_time', 'created']:
                    if isinstance(task[time_field], datetime.datetime) and not args.output_json:
                        task[time_field] = task[time_field].isoformat(timespec='seconds')
                    elif isinstance(task[time_field], datetime.datetime) and args.output_json:
                        task[time_field] = str(task[time_field].strftime('%Y-%m-%dT%H:%M:%S%z'))
                    elif not isinstance(task[time_field], datetime.datetime) and args.output_json:
                        task[time_field] = None
                    else:
                        if time_field == 'start_time':
                            task['start_time'] = 'Not started'
                        elif time_field == 'end_time':
                            task['end_time'] = 'Not finished'
                        else:
                            task['end_time'] = 'N/A'

                task_details = []

                for column in columns:
                    if not args.output_json and column == 'duration':
                        task_details.append(f"{task['duration']}s")
                        continue
                    elif not args.output_json and column == 'data' and task['data']:
                        task_details.append(f"{task['data']['channel_id']} - {task['data']['channel_label']}")
                    else:
                        if column in task:
                            task_details.append(task[column])
                        else:
                            logger.error(
                                f"Column \"{column}\" does not exist in the tasks data!\n" +
                                f"Available columns: {', '.join(task.keys())}"
                            )
                            return False

                all_tasks_details.append(task_details)

            # Output
            if args.output_json:
                all_tasks_details_list = []
                for task_details in all_tasks_details:
                    dict = {}
                    for i, header in enumerate(columns):
                        dict[str(header).lower()] = task_details[i]
                    all_tasks_details_list.append(dict)

                print(json.dumps(all_tasks_details_list, cls=lstasko.JSONEncoder, indent=4, sort_keys=False))
            elif tabulate:
                if not args.output_format:
                    table_format = 'plain'
                else:
                    table_format = args.output_format
                print(tabulate(all_tasks_details, headers=columns, tablefmt=table_format))
            elif not tabulate and not args.output_format:
                # Print a warning
                logger.warning(
                    "Tabulate is not installed! "
                    "Use `--json` flag or install `tabulate` to get reliable output."
                )
                # Print header
                print(",".join(columns))
                # Print data in CSV format
                for task_details in all_tasks_details:
                    for i, task_detail in enumerate(task_details):
                        task_details[i] = str(f"\"{task_detail}\"")
                    print(",".join(task_details))
            else:
                logger.error("Tabulate is not installed! Can't output results.")
                return False

            return True
    except (LSTaskoException, LSTaskoDatabaseNotConnectedException, LSTaskoNoRhnConfException) as e:
        logger.error(e)
        return False
    except BrokenPipeError:
        pass  # Ignore [Errno 32] Broken pipe
    except Exception as e:
        logger.error(f"Unknown error: {e}")
        return False


if __name__ == '__main__':
    if main():
        sys.exit(0)
    else:
        sys.exit(1)
