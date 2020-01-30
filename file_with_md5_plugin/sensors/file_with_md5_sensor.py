import os
import re
import glob
import hashlib

from airflow.utils.decorators import apply_defaults

from airflow.sensors.base_sensor_operator import BaseSensorOperator

from file_with_md5_plugin.hooks.file_with_md5_hook import FileWithMd5Hook


class FileWithMd5Sensor(BaseSensorOperator):
    """
    Given a conn_id (FileWithMd5Hook)

    Checks if a file matching then file_glob exists
        Returns True if a file matching then md5_glob also exists
        and the file's hash matches the md5sum

    :param conn_id: The conn_id with information about file_glob and md5_glob
    :type conn_id: str
    :param xcom_key_file_path: The xcom key for the xcom holding
        the full path to the file found through the glob
    :type xcom_key_file_path: str
    :param xcom_key_md5_path: The xcom key for the xcom holding
        the full path to the md5 file found through the glob
    :type xcom_key_md5_path: str
    """
    @apply_defaults
    def __init__(
            self,
            conn_id,
            xcom_key_file_path="file_with_md5_file_path",
            xcom_key_md5_path="file_with_md5_md5_path",
            *args, **kwargs
    ):
        super(FileWithMd5Sensor, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.xcom_key_file_path = xcom_key_file_path
        self.xcom_key_md5_path = xcom_key_md5_path

    def _get_file_from_glob(self, g):
        files = glob.glob(g)
        if not files:
            return None

        if len(files) > 1:
            msg = (f"glob {g} for {self.conn_id} matched more than one file: {files}, "
                   "refusing to proceed")
            self.log.warning(msg)
            return None

        return files[0]

    def _md5_file(self, f):
        h = hashlib.md5()
        with open(f, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def _read_md5file(self, md5_file, filename):
        """Reads the md5 of filename from md5_file. Matches based on os.path.basename."""

        md5s = {}
        try:
            with open(md5_file) as f:
                for line in f:
                    m = re.search(r"([a-f0-9]{32})\s*(.*?)$", line)
                    if not m:
                        msg = (f"md5_file is malformed on line: {line.strip()}, "
                               "refusing to proceed")
                        self.log.warning(msg)
                        return None

                    md5s[os.path.basename(m.group(2))] = m.group(1)
        except FileNotFoundError:
            msg = (f"md5_file {md5_file} can't be found, "
                   "refusing to proceed")
            self.log.warning(msg)
            return None

        h = md5s.get(os.path.basename(filename))
        if h is None:
            msg = (f"md5_file {md5_file} does not contain hash for {filename}, "
                   "refusing to proceed")
            self.log.warning(msg)
            return None

        return h

    def _poke(self, file_glob, md5_glob):
        """Returns the file that is found to match file_glob, and None in any error case"""
        file_ = self._get_file_from_glob(file_glob)
        if file_ is None:
            return None

        md5_file = self._get_file_from_glob(md5_glob)
        if md5_file is None:
            return None

        expected_md5 = self._read_md5file(md5_file, file_)
        if expected_md5 is None:
            return None

        actual_md5 = self._md5_file(file_)
        if expected_md5 == actual_md5:
            return (file_, md5_file)

        msg = (f"{file_}'s md5 {actual_md5} does not match md5 in file: {expected_md5}, "
               "refusing to proceed")
        self.log.warning(msg)
        return None


    def poke(self, context):
        hook = FileWithMd5Hook(self.conn_id)
        file_glob = hook.get_file_glob()
        md5_glob = hook.get_md5_glob()

        ret = self._poke(file_glob, md5_glob)
        if ret is None:
            return False

        file_, md5_file = ret

        ti = context["task_instance"]
        ti.xcom_push(key=self.xcom_key_file_path, value=file_)
        ti.xcom_push(key=self.xcom_key_md5_path, value=md5_file)

        return True
