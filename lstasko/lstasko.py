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
from .exceptions import LSTaskoException, LSTaskoDatabaseNotConnectedException, LSTaskoNoRhnConfException


class LSTasko:
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
                    channel_data = LSTasko._hack_parse_reposync_data(data)
                    return channel_data
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
            # self.logger.error(f"Could not open rhn.conf: {e}")
            raise LSTaskoNoRhnConfException(f"Could not open rhn.conf: {e}")

    def _hack_parse_reposync_data(self, data: bytes):
        """HACK: Converts reposync data bytes into list of dicts containing channel_id and channel_label"""
        # Specification: https://docs.oracle.com/javase/8/docs/platform/serialization/spec/protocol.html
        # Object Stream Constants: TC_STRING = 0x74 ('t'), TC_ENDBLOCKDATA = 0x78 ('x')
        # Data format: magic_bytes ... field_name [TC_STRING] value_length_16be field_value [TC_ENDBLOCKDATA]
        # Single channel repo-sync data: \xac\xed\x00\x05...channel_idt\x00\x03123x
        # FIXME: We currently handle only single channel repo-sync data.
        magic_bytes = b'\xac\xed\x00\x05'  # Java Object Serialization Stream magic bytes
        tc_string = b'\x74'  # 't'

        if (data[0:len(magic_bytes)] == magic_bytes):
            self.logger.debug("HACK: Parse repo-sync data")
            self.logger.debug(f"Raw data: {data}")

            string_pattern = 'channel_id'
            byte_pattern = str.encode(string_pattern)
            offset = data.find(byte_pattern) + len(byte_pattern)

            if offset:
                object_tc = data[offset:offset+1]
                object_tc_hex = hex(int.from_bytes(object_tc, "big"))
                self.logger.debug(f"Offset: {offset}, Stream constant type: {object_tc} ({object_tc_hex})")
                if (object_tc == tc_string):  # single channel id
                    value_length = int.from_bytes(data[offset+2:offset+3], "big")  # channel id number length
                    value_offset = offset + int.from_bytes(data[offset+1:offset+3], "big")  # channel id value offset
                    self.logger.debug(
                        f"Value length: {value_length}, "
                        f"Value: {data[value_offset:value_offset+value_length]}"
                    )
                    channel_id = int(data[value_offset:value_offset+value_length])
                    channel_details = self.get_channel_details(channel_id)
                    if channel_details:
                        self.logger.debug("Repo-sync information retrieved successfully!")
                    return [
                        {
                            'channel_id': int(channel_details['channel_id']),
                            'channel_label': str(channel_details['channel_label'])
                        }
                    ]
                else:
                    self.logger.debug("Data is not from a single channel repo-sync. Can't parse!")

        return None

    def get_reposync_details(self, data: bytes):
        """Get repo-sync task details from task bytes"""
        return self._hack_parse_reposync_data(data)

    def get_channel_label(self, channel_ids: Union[int, list]):
        """Get channel label(s) as a string or list of dicts containing channel_id and channel_label"""

        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        if isinstance(channel_ids, list):
            return_type = list
            for i, channel_id in enumerate(channel_ids):
                channel_ids[i] = psycopg2.sql.Literal(int(channel_id))
        elif isinstance(channel_ids, int):
            return_type = int
            channel_ids = [psycopg2.sql.Literal(channel_ids)]
        else:
            raise ValueError("Invalid argument value type!")

        if len(channel_ids) == 0:
            raise ValueError("Argument must be list (of ints) or singular int!")

        query = psycopg2.sql.SQL(
            "SELECT DISTINCT id, label "
            "FROM rhnChannel "
            "WHERE id IN ("
            f"{psycopg2.sql.SQL(', ').join(channel_ids).as_string(self._db_connection)}"
            ") "
            "ORDER BY label"
        )

        self._db_cursor.execute(query)

        result = self._db_cursor.fetchall()

        if not result:
            return None

        if return_type == int:
            self.logger.debug(f"Result single channel: {result}")
            return str(dict(result[0])['label'])
        else:
            self.logger.debug(f"Result multiple channels: {result}")
            channels = []
            for row in result:
                channels.append({'channel_id': int(row['id']), 'channel_label': str(row['label'])})
            return channels

    def get_channel_id(self, channel_labels: Union[str, list]):
        """Get channel id(s) as an int or list of dicts containing channel_id and channel_label"""

        if not self._db_cursor:
            raise LSTaskoDatabaseNotConnectedException()

        if isinstance(channel_labels, list):
            return_type = list
            for i, channel_label in enumerate(channel_labels):
                channel_labels[i] = psycopg2.sql.Literal(str(channel_label))
        elif isinstance(channel_labels, str):
            return_type = int
            channel_labels = [psycopg2.sql.Literal(channel_labels)]
        else:
            raise ValueError("Invalid argument value type!")

        if len(channel_labels) == 0:
            raise ValueError("Argument must be list (of strings) or singular string!")

        query = psycopg2.sql.SQL(
            "SELECT DISTINCT id, label "
            "FROM rhnChannel "
            "WHERE label IN ("
            f"{psycopg2.sql.SQL(', ').join(channel_labels).as_string(self._db_connection)}"
            ") "
            "ORDER BY id"
        )

        self._db_cursor.execute(query)

        result = self._db_cursor.fetchall()

        if not result:
            return None

        if return_type == int:
            self.logger.debug(f"Result single channel: {result}")
            return int(dict(result[0])['id'])
        else:
            self.logger.debug(f"Result multiple channels: {result}")
            channels = []
            for row in result:
                channels.append({'channel_id': int(row['id']), 'channel_label': str(row['label'])})
            return channels

    def get_channel_details(self, channel: Union[int, str]):
        channel_id = None
        channel_label = None
        if isinstance(channel, int):
            channel_id = channel
            channel_label = self.get_channel_label(int(channel_id))
        elif isinstance(channel, str):
            channel_label = channel
            channel_id = self.get_channel_id(channel_label)
        else:
            raise ValueError("Channel argument must be int (id) or str (label)!")

        return {'channel_id': int(channel_id), 'channel_label': str(channel_label)}

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
