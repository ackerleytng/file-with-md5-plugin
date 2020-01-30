from airflow.plugins_manager import AirflowPlugin

from file_with_md5_plugin.hooks.file_with_md5_hook import FileWithMd5Hook
from file_with_md5_plugin.sensors.file_with_md5_sensor import FileWithMd5Sensor


class FileWithMd5Plugin(AirflowPlugin):
    name = "file_with_md5"

    hooks = [FileWithMd5Hook]
    sensors = [FileWithMd5Sensor]
