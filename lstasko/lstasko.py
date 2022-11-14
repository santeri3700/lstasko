#
# Display information about Taskomatic tasks
#
# Copyright (c) 2022 Santeri Pikarinen <santeri3700>
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
import logging
import datetime
import json
from typing import Union
from decimal import Decimal
import psycopg2
import psycopg2.sql
import psycopg2.extras
import psycopg2.extensions
import javaobj.v2 as javaobj
from .exceptions import LSTaskoException, LSTaskoDatabaseNotConnectedException, \
                        LSTaskoNoRhnConfException, LSTaskoChannelNotFoundException


class LSTasko:
    version = (0, 2, 0)

    def __init__(self, db_conn_str: str = None):
        self.logger = logging.getLogger('lstasko')
        self._db_conn_str = db_conn_str
        self._db_connection = None
        self._db_cursor = None
        # Disable tracebacks in non-debug mode
        if not self.logger.isEnabledFor(logging.DEBUG):
            sys.tracebacklimit = 0

    def __enter__(self):
        if self._db_conn_str:
            return self.open(db_connection_string=self._db_conn_str)
        else:
            raise LSTaskoException("No PostgreSQL connection string provided! Can't open database connection.")

    def __exit__(self, exc_type, exc_value, traceback):
        if self._db_cursor:
            self.logger.debug("Closing DB...")
            self._db_close()

    def __del__(self):
        if self._db_cursor:
            self.logger.debug("Closing DB...")
            self._db_close()

    class JSONEncoder(json.JSONEncoder):
        """Taskomatic task data structure JSON encoder"""
        def default(self, data):
            if isinstance(data, datetime.datetime):
                return str(data.strftime('%Y-%m-%dT%H:%M:%S%z'))
            if isinstance(data, memoryview):
                return str(data)
            if isinstance(data, bytes):
                try:
                    reposync_details = LSTasko.get_reposync_details(LSTasko, data)
                    if reposync_details:
                        return reposync_details
                    else:
                        return None
                except Exception:
                    logging.getLogger('lstasko').debug("Unknown bytes parsing error!", exc_info=True)
                    return str(data)
            else:
                return super().default(data)

    def open(self, db_connection_string: str = None, db_host: str = None,
             db_name: str = None, db_user: str = None, db_password: str = None,
             db_port: int = 5432, db_sslmode: bool = False, db_sslrootcert: str = None):

        if db_connection_string:
            db_opened = self._db_open(db_connection_string)
        elif not db_connection_string and db_host and db_name and db_user:
            db_conn_str = self._get_db_conn_str(
                db_host=db_host,
                db_name=db_name,
                db_password=db_password,
                db_port=db_port,
                db_sslmode=db_sslmode,
                db_sslrootcert=db_sslrootcert
            )
            db_opened = self._db_open(db_conn_str)
        else:
            raise LSTaskoException(
                "Invalid database connection argument(s)! "
                "Arguments db_host, db_name and db_user are required."
            )

        if db_opened:
            return self

    def _db_open(self, db_connection_string: str):
        try:
            # Convert Decimal to int/float
            psycopg2.extensions.register_type(psycopg2.extensions.new_type(
                psycopg2.extensions.DECIMAL.values,
                'LSTASKODECCONV',
                lambda value, curs: int(value) if value is not None and Decimal(value) % 1 == 0
                else (float(value) if value is not None and Decimal(value) % 1 != 0 else None)
                )
            )
            self._db_connection = psycopg2.connect(db_connection_string)
            self._db_cursor = self._db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except psycopg2.Warning as w:
            self.logger.warning(f"Database connection warning! Warning: {w}")
            pass

        return True

    def _db_close(self):
        try:
            self._db_cursor.close()
            self._db_cursor = None
            self._db_connection.close()
            self._db_connection = None
            self.logger.debug("DB Closed!")
        except psycopg2.Warning as w:
            self.logger.warning(f"Database closure warning! Warning: {w}")
            pass

        return True

    def _get_db_conn_str(self, db_host: str, db_name: str,
                         db_user: str, db_password: str = None, db_port: int = 5432,
                         db_sslmode: str = None, db_sslrootcert: str = None
                         ):
        # Construct a connection string
        db_conn_str = ""
        if db_host:
            db_conn_str += f"host={db_host} "
        if db_port:
            db_conn_str += f"port={db_port} "
        if db_name:
            db_conn_str += f"dbname={db_name} "
        if db_user:
            db_conn_str += f"user={db_user} "
        if db_password:
            db_conn_str += f"password={db_password} "
        if db_sslmode:
            if db_sslrootcert:
                db_conn_str += "sslmode=verify-full "
                db_conn_str += f"sslrootcert={db_sslrootcert} "
            else:
                raise LSTaskoException("Argument db_sslmode must be provided with argument db_sslrootcert.")

        if db_conn_str:
            return db_conn_str
        else:
            return False

    def get_rhn_db_conn_str(self=None, rhn_conf_path: str = '/etc/rhn/rhn.conf'):
        """ Get rhn.conf dababase information if available """
        try:
            with open(rhn_conf_path, 'r') as file:
                rhn_conn_db_str = ''
                rhn_conf = {
                    'db_host': None,
                    'db_port': None,
                    'db_name': None,
                    'db_user': None,
                    'db_password': None,
                    'db_ssl_enabled': None,
                    'db_sslrootcert': None
                }
                keys = tuple(rhn_conf)
                lines = file.readlines()
                for line in lines:
                    if line.startswith(keys):
                        key = line.split('=')[0].strip()
                        value = line.split('=')[1].strip()
                        rhn_conf[key] = value

                # Construct a connection string
                if rhn_conf['db_host']:
                    rhn_conn_db_str += f"host={rhn_conf['db_host']} "
                if rhn_conf['db_port']:
                    rhn_conn_db_str += f"port={rhn_conf['db_port']} "
                if rhn_conf['db_name']:
                    rhn_conn_db_str += f"dbname={rhn_conf['db_name']} "
                if rhn_conf['db_user']:
                    rhn_conn_db_str += f"user={rhn_conf['db_user']} "
                if rhn_conf['db_password']:
                    rhn_conn_db_str += f"password={rhn_conf['db_password']}"
                if rhn_conf['db_ssl_enabled'] and rhn_conf['db_sslrootcert']:
                    rhn_conn_db_str += " sslmode=verify-full"
                    rhn_conn_db_str += f" sslrootcert={rhn_conf['db_sslrootcert']}"
                if rhn_conn_db_str:
                    return rhn_conn_db_str
                else:
                    return None
        except (FileNotFoundError, PermissionError, OSError) as e:
            raise LSTaskoNoRhnConfException(f"Could not open rhn.conf: {e}")

    def get_reposync_details(self, data: bytes):
        """Get repo-sync task details from task bytes"""
        details = {
            'no-errata': False,
            'latest': False,
            'sync-kickstart': False,
            'fail': False,
            'channels': []
        }

        try:
            obj = javaobj.loads(data)
            for class_definition, annotations in obj.annotations.items():
                # Skip everything that is not a Java HashMap
                if class_definition.name != "java.util.HashMap":
                    continue

                for index, annotation in enumerate(annotations):
                    # Multi-channel repo-sync
                    if annotation == 'channel_ids':
                        for subclass_definition, subannotations in annotations[index+1].annotations.items():
                            if len(subannotations) > 1:
                                self.logger.debug(f"subclass_definition: {subclass_definition}")
                                for channel_id in subannotations[1:]:
                                    # Convert JavaString to int
                                    channel_id = int(str(channel_id))

                                    # Append to details
                                    channel_details = self.get_channel_details(channel_id)
                                    details['channels'].append(channel_details)

                    # Single-channel repo-sync
                    elif annotation == 'channel_id':
                        # Convert JavaString to int
                        channel_id = int(str(annotations[index+1]))

                        # Append to details
                        channel_details = self.get_channel_details(channel_id)
                        details['channels'].append(channel_details)
                    # Repo-sync extra properties
                    elif annotation in ['no-errata', 'latest', 'sync-kickstart', 'fail']:
                        # Convert JavaString "true" and "false" to bool
                        details[annotation] = True if str(annotations[index+1]).lower() == 'true' else False
        except Exception as e:
            self.logger.debug(f"Channel repo-sync details could not be parsed! Error: {e}", exc_info=True)
            pass

        return details

    def get_channel_name(self, channel_identifiers: Union[int, str, list], ignore_missing: bool = False):
        """Get channel name(s) as a string or list of strings"""

        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        channel_details = self.get_channel_details(channel_identifiers, ignore_missing)

        if isinstance(channel_details, dict):
            return str(channel_details['name'])
        elif isinstance(channel_details, list):
            results = list(channel_identifiers)
            updated = []
            for single_channel_details in channel_details:
                for i, identifier in enumerate(results):

                    if isinstance(identifier, str) and identifier == str(single_channel_details['label']):
                        results[i] = str(single_channel_details['name'])
                        updated.append(i)
                    elif isinstance(identifier, int) and identifier == int(single_channel_details['id']):
                        results[i] = str(single_channel_details['name'])
                        updated.append(i)

            # Set not found identifiers to None
            for i in range(len(results)):
                if i not in updated:
                    results[i] = None

            return results

    def get_channel_label(self, channel_identifiers: Union[int, str, list], ignore_missing: bool = False):
        """Get channel labels(s) as a string or list of strings"""

        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        channel_details = self.get_channel_details(channel_identifiers, ignore_missing)

        if isinstance(channel_details, dict):
            return str(channel_details['label'])
        elif isinstance(channel_details, list):
            results = list(channel_identifiers)
            updated = []
            for single_channel_details in channel_details:
                for i, identifier in enumerate(results):

                    if isinstance(identifier, str) and identifier == str(single_channel_details['label']):
                        results[i] = str(single_channel_details['label'])
                        updated.append(i)
                    elif isinstance(identifier, int) and identifier == int(single_channel_details['id']):
                        results[i] = str(single_channel_details['label'])
                        updated.append(i)

            # Set not found identifiers to None
            for i in range(len(results)):
                if i not in updated:
                    results[i] = None

            return results

    def get_channel_id(self, channel_identifiers: Union[int, str, list], ignore_missing: bool = False):
        """Get channel id(s) as an int or list of ints"""

        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        channel_details = self.get_channel_details(channel_identifiers, ignore_missing)

        if isinstance(channel_details, dict):
            return int(channel_details['id'])
        elif isinstance(channel_details, list):
            results = list(channel_identifiers)
            updated = []
            for single_channel_details in channel_details:
                for i, identifier in enumerate(results):

                    if isinstance(identifier, str) and identifier == str(single_channel_details['label']):
                        results[i] = int(single_channel_details['id'])
                        updated.append(i)
                    elif isinstance(identifier, int) and identifier == int(single_channel_details['id']):
                        results[i] = int(single_channel_details['id'])
                        updated.append(i)

            # Set not found identifiers to None
            for i in range(len(results)):
                if i not in updated:
                    results[i] = None

            return results

    def get_channel_details(self, channel_identifiers: Union[int, str, list], ignore_missing: bool = False):
        """Get channel details as a dict or list"""

        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        # Items used in the SQL query
        search_items = {
            "labels": [],
            "ids": []
        }

        # Parse channel identifiers (ids and labels) and determine return type (dict or list)
        if isinstance(channel_identifiers, list):  # List of channel ids and/or labels
            return_type = list  # Return a list of channel details
            for channel_identifier in channel_identifiers:
                if isinstance(channel_identifier, int):  # Channel ID
                    search_items['ids'].append(int(channel_identifier))
                elif isinstance(channel_identifier, str):  # Channel label
                    search_items['labels'].append(str(channel_identifier))
        elif isinstance(channel_identifiers, str):  # Single channel label
            return_type = dict  # Return a single channel details dict
            search_items['labels'].append(str(channel_identifiers))
        elif isinstance(channel_identifiers, int):  # Single channel ID
            return_type = dict  # Return a single channel details dict
            search_items['ids'].append(int(channel_identifiers))
        else:
            raise ValueError("Invalid argument value type!")

        if len(search_items["ids"]) == 0 and len(search_items["labels"]) == 0:
            raise ValueError("Argument must be a channel label or a channel id or a list of those!")

        # Convert ids to list of Literals
        channel_ids = []
        for channel_id in search_items["ids"]:
            channel_ids.append(psycopg2.sql.Literal(channel_id))

        # Convert labels to list of Literals
        channel_labels = []
        for channel_label in search_items["labels"]:
            channel_labels.append(psycopg2.sql.Literal(channel_label))

        # Construct query
        query = "SELECT * FROM rhnChannel WHERE "

        # Search for channel IDs
        if len(channel_ids) > 0:
            channel_ids_list = psycopg2.sql.SQL(', ').join(channel_ids).as_string(self._db_connection)
            query += f"id IN ({channel_ids_list}) "

        # Search for channel labels
        if len(channel_labels) > 0:
            channel_labels_list = psycopg2.sql.SQL(', ').join(channel_labels).as_string(self._db_connection)
            # Add OR condition if ids are also being searched
            if len(channel_ids) > 0:
                query += "OR "
            query += f"label IN ({channel_labels_list}) "

        # Execute query and fetch all results
        self._db_cursor.execute(psycopg2.sql.SQL(query))
        result = self._db_cursor.fetchall()

        if not result:
            return None
        elif len(result) < (len(channel_ids) + len(channel_labels)):
            # Less results than search items
            if not ignore_missing:
                found_ids = []
                found_labels = []
                missing = []

                for row in result:
                    if int(row['id']) in search_items["ids"]:
                        found_ids.append(int(row['id']))
                    if str(row['label']) in search_items["labels"]:
                        found_labels.append(str(row['label']))

                for channel_id in search_items["ids"]:
                    if int(channel_id) not in found_ids:
                        missing.append(int(channel_id))

                for channel_label in search_items["labels"]:
                    if str(channel_label) not in found_labels:
                        missing.append(str(channel_label))

                # Raise with list of missing channel identifiers
                raise LSTaskoChannelNotFoundException(missing)

        # Return channel details per return type
        if return_type == dict:
            self.logger.debug(f"Result single channel: {result}")
            return dict(result[0])
        else:
            self.logger.debug(f"Result multiple channels: {result}")
            return list(result)

    def get_task(self, task: Union[int, list]):
        """Get Taskomatic task(s) per task id(s)"""
        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        task_ids = []
        if isinstance(task, list):
            return_type = list
            for task_id in task:
                task_ids.append(psycopg2.sql.Literal(int(task_id)))
        elif isinstance(task, int):
            return_type = dict
            task_ids = [psycopg2.sql.Literal(task)]
        else:
            raise ValueError("Invalid argument value type!")

        query = psycopg2.sql.SQL(
            "SELECT "
            "run.id AS id, "
            "run.org_id AS org_id, "
            "task.name AS name, "
            "task.class AS class, "
            "run.status AS status, "
            "run.created AS created, "
            "run.start_time AS start_time, "
            "run.end_time AS end_time, "
            "schedule.job_label AS job_label, "
            "schedule.cron_expr AS cron_expr, "
            "bunch.id AS bunch_id, "
            "bunch.name AS bunch_name, "
            "bunch.description AS bunch_desc, "
            "bunch.org_bunch AS bunch_org, "
            "schedule.data AS data, "
            "run.std_output_path AS stdout_file, "
            "run.std_error_path AS stderr_file "
            "FROM rhnTaskoSchedule schedule "
            "JOIN rhnTaskoRun run ON run.schedule_id = schedule.id "
            "JOIN rhnTaskoTemplate template ON template.id = run.template_id "
            "JOIN rhnTaskoTask task ON task.id = template.task_id "
            "JOIN rhnTaskoBunch bunch ON bunch.id = schedule.bunch_id "
            "WHERE run.id IN ("
            f"{psycopg2.sql.SQL(', ').join(task_ids).as_string(self._db_connection)}"
            ") "
            "ORDER BY start_time, end_time ASC"
        )

        self._db_cursor.execute(query)

        row = self._db_cursor.fetchone()

        result = []

        while row is not None:
            if 'data' in dict(row):
                if dict(row)["data"] is not None:
                    row["data"] = bytes(dict(row)["data"])
                else:
                    row["data"] = dict(row)["data"]
            else:
                row["data"] = None
            result.append(dict(row))
            row = self._db_cursor.fetchone()

        if return_type == dict and len(result) == 1:
            return result[0]
        elif return_type == dict and len(result) > 0:
            self.logger.warning("More than one task found. Returning a list instead.")
        return result

    def get_all_tasks(self):
        """Get list of Taskomatic tasks as dicts"""

        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        # TODO: Optimize query to only get requested fields (args.filter_columns)
        # TODO: Add optional sorting (ORDER BY x, y ASC/DESC)
        self._db_cursor.execute(
            """
            SELECT
            run.id AS id,
            run.org_id AS org_id,
            task.name AS name,
            task.class AS class,
            run.status AS status,
            run.created AS created,
            run.start_time AS start_time,
            run.end_time AS end_time,
            schedule.job_label AS job_label,
            schedule.cron_expr AS cron_expr,
            bunch.id AS bunch_id,
            bunch.name AS bunch_name,
            bunch.description AS bunch_desc,
            bunch.org_bunch AS bunch_org,
            schedule.data AS data,
            run.std_output_path AS stdout_file,
            run.std_error_path AS stderr_file
            FROM rhnTaskoSchedule schedule
            JOIN rhnTaskoRun run ON run.schedule_id = schedule.id
            JOIN rhnTaskoTemplate template ON template.id = run.template_id
            JOIN rhnTaskoTask task ON task.id = template.task_id
            JOIN rhnTaskoBunch bunch ON bunch.id = schedule.bunch_id
            ORDER BY start_time, end_time ASC
            """
        )

        row = self._db_cursor.fetchone()

        result = []

        while row is not None:
            if 'data' in dict(row):
                if dict(row)["data"] is not None:
                    row["data"] = bytes(dict(row)["data"])
                else:
                    row["data"] = dict(row)["data"]
            else:
                row["data"] = None
            result.append(dict(row))
            row = self._db_cursor.fetchone()

        return result
