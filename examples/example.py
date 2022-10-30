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
    # Loop though the list of tasks (dicts)
    for task in all_tasks:
        if task['name'] == 'channel-repodata':
            print(task['id'], task['name'], task['start_time'], task['end_time'])
            # 54321 channel-repodata 2022-10-30 12:00:00.000000+03:00 2022-10-30 12:01:00.000000+03:00
        elif task['name'] == 'repo-sync':
            details = lstasko.get_reposync_details(task['data'])  # task['data']: Bytes -> [{'channel_id': 123, 'channel_label': 'centos7-x86_64'}]
            task['data'] = details  # Replace Bytes data with dict
            print(json.dumps(task, indent=4, sort_keys=False, cls=lstasko.JSONEncoder))
            # {
            #     "id": 12345,
            #     "org_id": 1,
            #     "name": "repo-sync",
            #     "class": "com.redhat.rhn.taskomatic.task.RepoSyncTask",
            #     "status": "FINISHED",
            #     "created": "2022-10-30T12:15:00+0300",
            #     "start_time": "2022-10-30T12:15:01+0300",
            #     "end_time": "2022-10-30T12:30:30+0300",
            #     "job_label": "single-repo-sync-bunch-1",
            #     "cron_expr": "0 15 12 ? * *",
            #     "bunch_id": 8,
            #     "bunch_name": "repo-sync-bunch",
            #     "bunch_desc": "Used for syncing repos to a channel",
            #     "bunch_org": "Y",
            #     "data": [
            #         {
            #             "channel_id": 123,
            #             "channel_label": "centos7-x86_64"
            #         }
            #     ],
            #     "stdout_file": null,
            #     "stderr_file": null
            # }

    # Get Software Channel id(s) from label(s)
    single_channel_id = lstasko.get_channel_id('centos7-x86_64')  # int(123)
    multiple_channel_ids = lstasko.get_channel_id(['centos7-x86_64', 'centos7-x86_64-updates'])  # [{'channel_id': 123, 'channel_label': 'centos7-x86_64'}, ...]
    print(single_channel_id, multiple_channel_ids)

    # Get Software Channel label(s) from id(s)
    single_channel_label = lstasko.get_channel_label(single_channel_id)  # str('centos7-x86_64')
    multiple_channel_labels = lstasko.get_channel_label([123, 124])  # [{'channel_id': 123, 'channel_label': 'centos7-x86_64'}, ...]
    print(single_channel_label, multiple_channel_labels)

    # Get task details per task id
    single_task_details = lstasko.get_task(12345)  # dict
    multiple_tasks_details = lstasko.get_task([12345, 12346])  # list[dict]
    print(single_task_details, multiple_tasks_details)
